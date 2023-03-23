import argparse
import asyncio
import json
from pathlib import Path
from collections import defaultdict
import shutil
import time

import aiofiles
from loguru import logger

from codex_parser import PageParser, IndexParser
from network import OrnaGuideClient, OrnaCodexClient


GUIDE_INTERFACES = ['item', 'monster', 'pet']
CODEX_INTERFACES = ['items', 'classes', 'monsters', 'bosses', 'followers', 'raids', 'spells']  # buildings, dungeons
ORNA_CODEX_WORKERS = 32
PARSE_CODEX_WORKERS = 64

async def _fetch_codex_meta_iter(client: OrnaCodexClient.Client, interface: str):
    async for page in client.fetch_index_iter(interface):
        for item in IndexParser.parse_iter(page):
            yield item

async def fetch_meta_data(guide_meta_dir: str, codex_meta_dir: str, clean: bool = False):
    async with OrnaGuideClient.Client() as client:
        for interface in GUIDE_INTERFACES:
            logger.info(f'Fetching {interface} from OrnaGuide...')
            meta_data_path = Path(guide_meta_dir).joinpath(f'{interface}.json')
            if not clean and meta_data_path.exists():
                logger.info(f'{meta_data_path} exists, skip it')
                continue
            start = time.time()
            data = await client.fetch(interface, {})
            async with aiofiles.open(meta_data_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=4))
                logger.info(f'Cost {time.time() - start}s, Wrote {interface}.json')
    
    async with OrnaCodexClient.Client() as client:
        for interface in CODEX_INTERFACES:
            logger.info(f'Fetching {interface} from OrnaCodex...')
            meta_data_path = Path(codex_meta_dir).joinpath(f'{interface}.json')
            if not clean and meta_data_path.exists():
                logger.info(f'{meta_data_path} exists, skip it')
                continue
            start = time.time()
            data = [item async for item in _fetch_codex_meta_iter(client, interface)]
            async with aiofiles.open(meta_data_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=4))
                logger.info(f'Cost {time.time() - start}s, Wrote {interface}.json')


async def _fetch_codex(client: OrnaCodexClient.Client, data_dir: str, item: dict, sem: asyncio.Semaphore):
    # item = {'name': name, 'codex': codex}
    async with sem:
        logger.info(f"Get {item['codex']}")
        if item['codex'] is None:
            logger.info(f"Skip {item['name']}")
            return
        item_file_path = Path(data_dir).joinpath(f"{item['codex'].strip('/')}.html")
        if item_file_path.exists():
            logger.info(f'{item_file_path} exists, skip it')
            return
        item_file_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Fetching {item['codex']} from OrnaCodex...")
        codex_resp = await client.fetch(item['codex'], raw=True)
        if codex_resp.status_code == 404: # type: ignore
            logger.info(f"Skip {item['name']}")  # 404
            return
        async with aiofiles.open(item_file_path, 'w', encoding='utf-8') as f:
            await f.write(codex_resp.text) # type: ignore
            logger.info(f"Wrote {item_file_path}")


async def fetch_codex(guide_meta_dir: str, codex_meta_dir: str, codex_dir: str, lang: str, clean: bool = True):
    async with OrnaCodexClient.Client(lang=lang) as client:
        sem = asyncio.Semaphore(ORNA_CODEX_WORKERS)
        for interface in GUIDE_INTERFACES:
            logger.info(f'Fetching {interface} from OrnaGuide...')
            async with aiofiles.open(Path(guide_meta_dir).joinpath(f'{interface}.json'), 'r', encoding='utf-8') as f:
                meta_data = json.loads(await f.read())
            tasks = [asyncio.create_task(_fetch_codex(client, f'{codex_dir}/{lang}', item, sem)) for item in meta_data]
            for task in asyncio.as_completed(tasks):
                await task
            logger.info(f'Finished {interface}')

        for interface in CODEX_INTERFACES:
            logger.info(f'Fetching {interface} from OrnaCodex...')
            async with aiofiles.open(Path(codex_meta_dir).joinpath(f'{interface}.json'), 'r', encoding='utf-8') as f:
                meta_data = json.loads(await f.read())
            tasks = [asyncio.create_task(_fetch_codex(client, f'{codex_dir}/{lang}', item, sem)) for item in meta_data]
            for task in asyncio.as_completed(tasks):
                await task
            logger.info(f'Finished {interface}')
    logger.info(f'Finished all')


async def _parse_codex(input_path: Path, output_path: Path, sem: asyncio.Semaphore):
    async with sem:
        logger.info(f'Parsing {input_path}...')
        async with aiofiles.open(input_path, 'r', encoding='utf-8') as input:
            data_in = await input.read()
            loop = asyncio.get_event_loop()
            data_out = await loop.run_in_executor(
                None, PageParser.parse, data_in, '/'.join(['', *input_path.parts[-3:-1], input_path.stem, '']), True
            )
            if data_out is None:
                logger.info(f'Parse {input_path} failed')
                return 
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as output:
            await output.write(json.dumps(data_out, indent=4, ensure_ascii=False))

async def parse_codex(input_dir: str, output_dir: str):
    output_path = Path(output_dir).joinpath('codex')
    for type_dir in Path(input_dir).joinpath('codex').iterdir():
        output_type_dir = output_path.joinpath(type_dir.name)
        output_type_dir.mkdir(parents=True, exist_ok=True)
        tasks = []
        sem = asyncio.Semaphore(PARSE_CODEX_WORKERS)
        for file_path in type_dir.iterdir():
            if output_type_dir.joinpath(f'{file_path.stem}.json').exists():
                continue
            tasks.append(asyncio.create_task(_parse_codex(file_path, output_type_dir.joinpath(f'{file_path.stem}.json'), sem)))
        for task in asyncio.as_completed(tasks):
            await task
    logger.info(f'Finished all')


async def check_miss_codex(json_dir: str, codex_dir: str, lang: str, clean: bool = False):
    miss_codex_list = []
    check_interface = ['bosses', 'items', 'monsters', 'raids']
    for interface in check_interface:
        logger.info(f'Checking {interface}...')
        input_subdir = Path(json_dir).joinpath('codex', interface)
        for file_path in input_subdir.iterdir():
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
            drop = data.get('drop', [])
            for item_list in drop:
                for item in item_list['base']:
                    codex = item.get('codex') if isinstance(item, dict) else None
                    if codex is None or codex.split('/')[2] not in check_interface:
                        continue
                    item_path = Path(json_dir).joinpath(f'{codex.strip("/")}.json')
                    if not item_path.exists():
                        miss_codex_list.append(item)
                        logger.info(f'Found miss codex {item["name"]}(href: "{codex}")')
    logger.info('Downloading miss codex...')
    for item in miss_codex_list:
        async with OrnaCodexClient.Client(lang=lang) as client:
            await _fetch_codex(client, codex_dir, {'name': item['name'], 'codex': item['codex']}, asyncio.Semaphore(1))
            file_path = Path(codex_dir).joinpath(f'{item["codex"].strip("/")}.html')
            if not file_path.exists():
                logger.info(f'Fetch {item["name"]}(href: "{item["codex"]}") failed')
                continue
            logger.info(f'Parsing {file_path}...')
            await _parse_codex(file_path, Path(json_dir).joinpath(f'{item["codex"].strip("/")}.json'), asyncio.Semaphore(1))
    logger.info(f'Finished all')


async def build_index(input_dir: str, output_dir: str, base_lang: str = 'us-en'):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    base_dir = Path(input_dir).joinpath(base_lang)
    for interface in CODEX_INTERFACES:
        logger.info(f'Building {interface} Index...')
        input_subdir = Path(base_dir).joinpath('codex', interface)
        index = []
        meta = defaultdict(lambda: defaultdict(list))
        tag = defaultdict(list)
        drop = defaultdict(lambda: defaultdict(list))
        for file_path in input_subdir.iterdir():
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
            codex = data['codex'].strip('/').split('/')[-1]
            index.append({
                'name': data['name'],
                'rarity': data['rarity'],
                'icon': data['icon'],
                'description': data['description'],
                'codex': codex,
            })
            for s in data.get('meta', []):
                meta[s['name']][s['base']].append(codex)
            for t in data.get('tag', []):
                tag[t['name']].append(codex)
            for d in data.get('drop', []):
                for base in d.get('base', []):
                    drop[d['name']][base['codex' if base.get('codex') else 'name']].append(codex)
        async with aiofiles.open(Path(output_dir).joinpath(f'{interface}.index.json'), 'w', encoding='utf-8') as f:
            await f.write(json.dumps(index, indent=4, ensure_ascii=False))
        async with aiofiles.open(Path(output_dir).joinpath(f'{interface}.meta.json'), 'w', encoding='utf-8') as f:
            await f.write(json.dumps(meta, indent=4, ensure_ascii=False))
        async with aiofiles.open(Path(output_dir).joinpath(f'{interface}.tag.json'), 'w', encoding='utf-8') as f:
            await f.write(json.dumps(tag, indent=4, ensure_ascii=False))
        async with aiofiles.open(Path(output_dir).joinpath(f'{interface}.drop.json'), 'w', encoding='utf-8') as f:
            await f.write(json.dumps(drop, indent=4, ensure_ascii=False))

async def build_translation(input_dir: str, output_dir: str, base_lang: str = 'us-en'):
    translation_dir = Path(output_dir).joinpath('translation')
    translation_dir.mkdir(parents=True, exist_ok=True)
    base_dir = Path(input_dir).joinpath(base_lang)
    langs = [x.name for x in Path(input_dir).iterdir() if x.is_dir() and x.name != base_lang]
    for interface in CODEX_INTERFACES:
        logger.info(f'Building {interface} Translation...')
        index = defaultdict(lambda: defaultdict(list))
        meta = defaultdict(dict)
        stat = defaultdict(dict)
        tag = defaultdict(dict)
        drop = defaultdict(dict)
        for base in base_dir.joinpath('codex', interface).iterdir():
            async with aiofiles.open(base, 'r', encoding='utf-8') as f:
                base_data = json.loads(await f.read())
            codex = base_data['codex']
            for lang in langs:
                async with aiofiles.open(Path(input_dir).joinpath(lang, f'{codex.strip("/")}.json'), 'r', encoding='utf-8') as f:
                    lang_data = json.loads(await f.read())
                index[lang][codex] = [
                    lang_data['name'], lang_data['description'],
                ]
                for b, l in zip(base_data.get('meta', []), lang_data.get('meta', [])):
                    meta[lang][b['name']] = l['name']
                    if b['base'] == l['base']:
                        continue
                    meta[lang][b['base']] = l['base']
                for b, l in zip(base_data.get('stat', []), lang_data.get('stat', [])):
                    stat[lang][b['name']] = l['name']
                    if b.get('base') != l.get('base'):
                        stat[lang][b['base']] = l['base']
                for b, l in zip(base_data.get('tag', []), lang_data.get('tag', [])):
                    tag[lang][b['name']] = l['name']
                for b, l in zip(base_data.get('drop', []), lang_data.get('drop', [])):
                    drop[lang][b['name']] = l['name']
                    for bb, ll in zip(b.get('base', []), l.get('base', [])):
                        if bb.get('codex') or bb['name'] == ll['name']:
                            continue
                        drop[lang][bb['name']] = ll['name']
        for lang in langs:
            logger.info(f'Writing {interface} Translation for {lang}...')
            async with aiofiles.open(translation_dir.joinpath(f'{interface}.{lang}.json'), 'w', encoding='utf-8') as f:
                await f.write(json.dumps({
                    'index': index[lang],
                    'meta': meta[lang],
                    'stat': stat[lang],
                    'tag': tag[lang],
                    'drop': drop[lang],
                }, indent=4, ensure_ascii=False))


async def build_database(input_dir: str, output_db: str):
    # ToDo: build database
    import aiosqlite
    async with aiosqlite.connect(output_db) as db:
        pass



async def main():
    parser = argparse.ArgumentParser('Orna Codex Indexer')
    parser.add_argument('--clean', action='store_true', help='remove data before fetch')
    parser.add_argument('--lang', type=str, default='us-en', help='download language')
    parser.add_argument('--data-dir', type=str, default='playorna', help='data directory')

    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument('--fetch-meta', action='store_true', help='fetch meta data')
    action_group.add_argument('--fetch-codex', action='store_true', help='fetch codex data')
    action_group.add_argument('--parse-codex', action='store_true', help='parse codex data')
    action_group.add_argument('--check-miss', action='store_true', help='check missing codex')
    action_group.add_argument('--build-index', action='store_true', help='build codex index')
    action_group.add_argument('--all', action='store_true', help='fetch and parse all data')
    
    args = parser.parse_args()
    clean = args.clean
    lang = args.lang
    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    guide_meta_dir = data_dir.joinpath('guide_meta')
    codex_meta_dir = data_dir.joinpath('codex_meta')
    codex_data_dir = data_dir.joinpath('codex')
    codex_json_dir = data_dir.joinpath('json')
    codex_index_dir = data_dir.joinpath('index')
    
    if args.fetch_meta or args.all:
        if clean and guide_meta_dir.exists():
            shutil.rmtree(guide_meta_dir)
        if clean and codex_meta_dir.exists():
            shutil.rmtree(codex_meta_dir)
        guide_meta_dir.mkdir(parents=True, exist_ok=True)
        codex_meta_dir.mkdir(parents=True, exist_ok=True)
        await fetch_meta_data(
            guide_meta_dir=str(guide_meta_dir),
            codex_meta_dir=str(codex_meta_dir),
        )
    if args.fetch_codex or args.all:
        if clean and codex_data_dir.exists():
            shutil.rmtree(codex_data_dir)
        codex_data_dir.mkdir(parents=True, exist_ok=True)
        await fetch_codex(
            guide_meta_dir=str(guide_meta_dir),
            codex_meta_dir=str(codex_meta_dir),
            codex_dir=str(codex_data_dir),
            lang=lang, 
        )
    if args.parse_codex or args.all:
        if clean and codex_json_dir.joinpath(lang).exists():
            shutil.rmtree(codex_json_dir.joinpath(lang))
        codex_json_dir.joinpath(lang).mkdir(parents=True, exist_ok=True)
        await parse_codex(
            input_dir=str(codex_data_dir.joinpath(lang)),
            output_dir=str(codex_json_dir.joinpath(lang)),
        )
    if args.check_miss or args.all:
        await check_miss_codex(
            json_dir=str(codex_json_dir.joinpath(lang)),
            codex_dir=str(codex_data_dir.joinpath(lang)),
            lang=lang,
        )
    if args.build_index:
        if clean and codex_index_dir.exists():
            shutil.rmtree(codex_index_dir)
        codex_index_dir.mkdir(parents=True, exist_ok=True)
        await build_index(
            input_dir=str(codex_json_dir),
            output_dir=str(codex_index_dir),
        )
        await build_translation(
            input_dir=str(codex_json_dir),
            output_dir=str(codex_index_dir),
        )


if __name__ == '__main__':
    asyncio.run(main())