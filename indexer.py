import argparse
import asyncio
import json
from pathlib import Path
from collections import defaultdict
import time

import aiofiles
from loguru import logger

from codex_parser import PageParser, IndexParser
from network import OrnaGuideClient, OrnaCodexClient


GUIDE_INTERFACES = ['item', 'monster', 'pet']
CODEX_INTERFACES = ['items', 'classes', 'monsters', 'bosses', 'followers', 'raids', 'spells']  # buildings, dungeons
ORNA_CODEX_WORKERS = 32

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


async def _fetch_codex(client: OrnaCodexClient.Client, data_dir: str, item: dict, sem: asyncio.Semaphore, clean: bool = False):
    # item = {'name': name, 'codex': codex}
    async with sem:
        logger.info(f"Get {item['codex']}")
        if item['codex'] is None:
            logger.info(f"Skip {item['name']}")
            return
        item_file_path = Path(data_dir).joinpath(f"{item['codex'].strip('/')}.html")
        if not clean and item_file_path.exists():
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
            logger.info(f'Fetching {interface} from OrnaCodex...')
            async with aiofiles.open(Path(guide_meta_dir).joinpath(f'{interface}.json'), 'r', encoding='utf-8') as f:
                meta_data = json.loads(await f.read())
            tasks = [asyncio.create_task(_fetch_codex(client, f'{codex_dir}/{lang}', item, sem, clean)) for item in meta_data]
            for task in asyncio.as_completed(tasks):
                await task
            logger.info(f'Finished {interface}')

        for interface in CODEX_INTERFACES:
            logger.info(f'Fetching {interface} from OrnaCodex...')
            async with aiofiles.open(Path(codex_meta_dir).joinpath(f'{interface}.json'), 'r', encoding='utf-8') as f:
                meta_data = json.loads(await f.read())
            tasks = [asyncio.create_task(_fetch_codex(client, f'{codex_dir}/{lang}', item, sem, clean)) for item in meta_data]
            for task in asyncio.as_completed(tasks):
                await task
            logger.info(f'Finished {interface}')
    logger.info(f'Finished all')


async def parse_codex(input_dir: str, output_dir: str, clean: bool = False):
    output_path = Path(output_dir).joinpath('codex')
    for type_dir in Path(input_dir).joinpath('codex').iterdir():
        output_type_dir = output_path.joinpath(type_dir.name)
        output_type_dir.mkdir(parents=True, exist_ok=True)
        for file_path in type_dir.iterdir():
            logger.info(f'Parsing {file_path}...')
            if not clean and output_type_dir.joinpath(f'{file_path.stem}.json').exists():
                logger.info(f'{file_path} exists, skip it')
                continue
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as input:
                data_in = await input.read()
                data_out = await asyncio.to_thread(
                    PageParser.parse, data_in, '/'.join(['', *file_path.parts[-3:-1], file_path.stem, '']), raw_dict=True
                )
                if data_out is None:
                    logger.info(f'Parse {file_path} failed')
                    continue
            async with aiofiles.open(output_type_dir.joinpath(f'{file_path.stem}.json'), 'w', encoding='utf-8') as output:
                await output.write(json.dumps(data_out, indent=4, ensure_ascii=False)) # type: ignore
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
            for item_list in drop.values():
                for item in item_list:
                    item_href = item.get('href') if isinstance(item, dict) else None
                    if item_href is None or item_href.split('/')[2] not in check_interface:
                        continue
                    item_path = Path(json_dir).joinpath(f'{item_href.strip("/")}.json')
                    if not item_path.exists():
                        miss_codex_list.append(item)
                        logger.info(f'Found miss codex {item["name"]}(href: "{item_href}")')
    logger.info('Downloading miss codex...')
    for item in miss_codex_list:
        async with OrnaCodexClient.Client(lang=lang) as client:
            await _fetch_codex(client, codex_dir, {'name': item['name'], 'codex': item['href']}, asyncio.Semaphore(1), clean)
            file_path = Path(codex_dir).joinpath(f'{item["href"].strip("/")}.html')
            logger.info(f'Parsing {file_path}...')
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as input:
                data_in = await input.read()
                data_out = await asyncio.to_thread(
                    PageParser.parse, data_in, '/'.join(['', *file_path.parts[-3:-1], file_path.stem, '']), raw_dict=True
                )
                if data_out is None:
                    logger.info(f'Parse {file_path} failed')
                    continue
            async with aiofiles.open(Path(json_dir).joinpath(f'{item["href"].strip("/")}.json'), 'w', encoding='utf-8') as output:
                await output.write(json.dumps(data_out, indent=4, ensure_ascii=False)) # type: ignore
                logger.info(f'Parsed {file_path}')
    logger.info(f'Finished all')
                    

async def build_index(input_dir: str, output_dir: str, clean: bool = False, lang: str = 'us-en'):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for interface in CODEX_INTERFACES:
        logger.info(f'Building {interface} index...')
        index = []
        codex_sub_dir = Path(input_dir).joinpath(lang, 'codex', interface)
        for file_path in codex_sub_dir.iterdir():
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                data = json.loads(await f.read())
                index.append({
                    'name': data['name'],
                    'rarity': data['rarity'],
                    'description': data['description'],
                    'codex': data['codex'],
                    'icon': data['icon'],
                    'meta': [{item: True} if isinstance(item, str) else {item[0]: item[1]} for item in data['meta']],
                    'tag': data['tag'],
                })
        async with aiofiles.open(Path(output_dir).joinpath(f'{interface}.json'), 'w', encoding='utf-8') as f:
            await f.write(json.dumps({'total': len(index), 'row': index}, indent=4, ensure_ascii=False))
    logger.info(f'Finished all')


async def build_database(input_dir: str, output_db: str):
    # ToDo: build database
    import aiosqlite
    async with aiosqlite.connect(output_db) as db:
        pass



async def main():
    parser = argparse.ArgumentParser('Orna Codex Indexer')
    parser.add_argument('--clean', action='store_true', help='fetch again and overwrite')
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
    guide_meta_dir.mkdir(parents=True, exist_ok=True)
    codex_meta_dir.mkdir(parents=True, exist_ok=True)
    codex_data_dir.mkdir(parents=True, exist_ok=True)
    codex_json_dir.mkdir(parents=True, exist_ok=True)
    codex_index_dir.mkdir(parents=True, exist_ok=True)

    if args.fetch_meta or args.all:
        await fetch_meta_data(
            guide_meta_dir=str(guide_meta_dir),
            codex_meta_dir=str(codex_meta_dir),
            clean=clean
        )
    if args.fetch_codex or args.all:
        await fetch_codex(
            guide_meta_dir=str(guide_meta_dir),
            codex_meta_dir=str(codex_meta_dir),
            codex_dir=str(codex_data_dir),
            lang=lang, 
            clean=clean
        )
    if args.parse_codex or args.all:
        await parse_codex(
            input_dir=str(codex_data_dir.joinpath(lang)),
            output_dir=str(codex_json_dir.joinpath(lang)),
            clean=clean
        )
    if args.check_miss or args.all:
        await check_miss_codex(
            json_dir=str(codex_json_dir.joinpath(lang)),
            codex_dir=str(codex_data_dir.joinpath(lang)),
            lang=lang,
            clean=clean
        )
    if args.build_index:
        await build_index(
            input_dir=str(codex_json_dir),
            output_dir=str(codex_index_dir),
            lang=lang,
            clean=clean
        )


if __name__ == '__main__':
    import platform
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) # type: ignore
    asyncio.run(main())