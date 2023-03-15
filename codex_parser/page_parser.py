import re
from collections import defaultdict
from typing import Iterator, Tuple, Union

from lxml import etree

from .codex_types import CodexType

EFFECT_PATTERN = r'(?P<EFFECT>.+) \((?P<CHANCE>\d+%)\)'
effect_pattern = re.compile(pattern=EFFECT_PATTERN) # type: ignore
KV_PATTERN = r'(?P<KEY>.+)(:|：) (?P<VALUE>.+)'
kv_pattern = re.compile(pattern=KV_PATTERN) # type: ignore

class PageParser:

    @classmethod
    def drop_parse(cls, elems: list) -> dict:
        drop = defaultdict(list)
        handler = []
        for elem in elems:
            if elem.tag == 'h4':
                drop_header = elem.text.strip(':')
                drop[drop_header] = []
                handler = drop[drop_header]
            elif elem.attrib.get('class') == 'drop':
                drop_str = elem.xpath("string()").strip() 
                icon = elem.xpath('img')[0].attrib.get('src')
                ability = elem.xpath('../div[@class="emph"]')
                if elem.tag == 'a':
                    handler.append({
                        'href': elem.attrib.get('href'), 
                        'name': drop_str,
                        'icon': icon,
                    })
                else:
                    matches = effect_pattern.match(drop_str)
                    if matches:
                        handler.append({
                            'name':matches.group('EFFECT'), 
                            'chance': matches.group('CHANCE'),
                            'icon': icon,
                        })
                    elif ability:
                        handler.append({
                            'name': drop_str, 
                            'icon': icon,
                            'ability': ability[0].xpath("string()").strip(),
                        })
                    else:
                        handler.append({'name': drop_str, 'icon': icon})
        return dict(drop)
    

    @classmethod
    def kv_parse_iter(cls, elems: Iterator):
        for elem in elems:
            kv = elem.xpath("string()").strip()
            matches = kv_pattern.match(kv)
            if matches:
                yield ['_'.join(matches.group('KEY').split()), matches.group('VALUE')]
            else:
                yield kv
    
    @classmethod
    def meta_parse_iter(cls, elems: list):
        for elem in elems:
            span = elem.xpath('span')
            if len(span) > 0 and span[0].xpath('boolean(@class="exotic")'):
                yield ['exotic', span[0].xpath("string()").strip()]
                continue
            meta = elem.xpath("string()").strip()
            matches = kv_pattern.match(meta)
            if matches:
                yield ['_'.join(matches.group('KEY').split()), matches.group('VALUE')]
            else:
                yield meta

    @classmethod
    def stat_parse_iter(cls, elems: list):
        for elem in elems:
            stats = elem.attrib.get('class').split()
            if len(stats) > 1:
                yield [f'element:{stats[1]}', elem.xpath("string()").strip()]
                continue
            meta = elem.xpath("string()").strip()
            matches = kv_pattern.match(meta)
            if matches:
                yield ['_'.join(matches.group('KEY').split()), matches.group('VALUE')]
            else:
                yield meta

    @classmethod
    def description_parse(cls, pre_elems: list, div_elems: list, codex_type: str) -> Tuple[str, list, list]:
        description = pre_elems[0].xpath("string()").strip() if len(pre_elems) > 0 else ''
        meta_extra = []
        offhand = []
        if codex_type in {'items'} and len(div_elems) > 0:
            ability = div_elems[0].xpath("preceding-sibling::div[1]")[0].xpath("string()").strip().split(':')
            offhand = [ability[0], [{
                'name': ability[1],
                'ability': div_elems[0].xpath("string()").strip(),
            }]]
        if codex_type in {'bosses', 'monsters'} and len(div_elems) > 0:
            meta_extra = list(cls.kv_parse_iter(div_elems)) # type: ignore
        if codex_type in {'followers', 'raids', 'spells', 'classes'} and len(div_elems) > 0:
            description = div_elems[0].xpath("string()").strip()
            meta_extra = list(cls.kv_parse_iter(div_elems[1:])) # type: ignore
        return description, meta_extra, offhand

    @classmethod
    def parse(cls, html: str, codex: str, raw_dict: bool = False) -> Union[dict, CodexType, None]:
        parser = etree.HTMLParser(encoding='utf-8')
        page = etree.HTML(html, parser=parser)
        name = page.xpath('/html/body/div[@class="hero smaller"]/h1/text()')[0]
        if name == '404':
            # 规避本地下载的404页面
            return None
        
        icon_elem = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/div[@class="codex-page-icon"]/img')[0]
        icon_rarity = icon_elem.attrib['class'].strip() if icon_elem.xpath('boolean(@class)') else ''
        icon = icon_elem.attrib['src']

        description_pre_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/pre[contains(@class,"codex-page-description")]')
        description_div_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/div[contains(@class,"codex-page-description")]')
        description, meta_extra, offhand = cls.description_parse(description_pre_elems, description_div_elems, codex.split('/')[2])

        meta_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//div[@class="codex-page-meta"]')
        meta = meta_extra + list(cls.meta_parse_iter(meta_elems))

        tag_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//div[@class="codex-page-tag"]')
        tag = [i[2:] for i in cls.kv_parse_iter(tag_elems)]

        stat_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/div[@class="codex-stats"]//div[contains(@class,"codex-stat")]')
        stat = list(cls.stat_parse_iter(stat_elems))

        drop_elems = page.xpath(
            '/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/h4'
            '|/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//*[@class="drop"]'
        )
        drop = cls.drop_parse(drop_elems)
        if offhand:
            drop[offhand[0]] = offhand[1]

        if raw_dict:
            return {
                'codex': codex,
                'name': name,
                'rarity': icon_rarity,
                'icon': icon,
                'description': description,
                'meta': meta,
                'tag': tag,
                'stat': stat,
                'drop': drop,
            }
        else:
            return CodexType(
                codex=codex,
                name=name,
                rarity=icon_rarity,
                icon=icon,
                description=description,
                meta=meta,
                tag=tag,
                stat=stat,
                drop=drop,
            )
    

