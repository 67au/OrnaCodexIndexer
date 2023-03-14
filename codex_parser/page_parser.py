import re
from collections import defaultdict
from typing import Iterator, Tuple, Union

from lxml import etree

from .codex_types import CodexType

EFFECT_PATTERN = r'(?P<EFFECT>.+) \((?P<CHANCE>\d+%)\)'
effect_pattern = re.compile(pattern=EFFECT_PATTERN) # type: ignore
META_PATTERN = r'(?P<KEY>.+)(:|：) (?P<VALUE>.+)'
meta_pattern = re.compile(pattern=META_PATTERN) # type: ignore

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
                if elem.tag == 'a':
                    handler.append({
                        'href': elem.attrib.get('href'), 
                        'name': elem.xpath("string()").strip(),
                    })
                else:
                    matches = effect_pattern.match(elem.xpath("string()").strip())
                    if matches:
                        handler.append([matches.group('EFFECT'), matches.group('CHANCE')])
        return dict(drop)
    

    @classmethod
    def kv_parse_iter(cls, elems: Iterator):
        for elem in elems:
            meta = elem.xpath("string()").strip()
            matches = meta_pattern.match(meta)
            if matches:
                yield [matches.group('KEY'), matches.group('VALUE')]
            else:
                yield meta

    @classmethod
    def description_parse(cls, pre_elems: list, div_elems: list, codex_type: str) -> Tuple[str, list, list]:
        description = pre_elems[0].xpath("string()").strip() if len(pre_elems) > 0 else ''
        meta_extra = []
        stat_extra = []
        if codex_type in {'items'} and len(div_elems) > 0:
            stat_extra = [[div_elems[0].xpath("preceding-sibling::div[1]")[0].xpath("string()").strip(), div_elems[0].xpath("string()").strip()]]
        if codex_type in {'bosses', 'monster'} and len(div_elems) > 0:
            meta_extra = list(cls.kv_parse_iter(div_elems)) # type: ignore
        if codex_type in {'followers', 'raids', 'spells', 'classes'} and len(div_elems) > 0:
            description = div_elems[0].xpath("string()").strip()
            meta_extra = list(cls.kv_parse_iter(div_elems[1:])) # type: ignore
        return description, meta_extra, stat_extra

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
        description_div_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//div[contains(@class,"codex-page-description")]')
        description, meta_extra, stat_extra = cls.description_parse(description_pre_elems, description_div_elems, codex.split('/')[2])

        meta_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//div[@class="codex-page-meta"]')
        meta = meta_extra + list(cls.kv_parse_iter(meta_elems))

        tag_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//div[@class="codex-page-tag"]')
        tag = list(cls.kv_parse_iter(tag_elems))

        stat_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/div[@class="codex-stats"]//div[contains(@class,"codex-stat")]')
        stat = list(cls.kv_parse_iter(stat_elems)) + stat_extra

        drop_elems = page.xpath(
            '/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/h4'
            '| /html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//*[@class="drop"]'
        )
        drop = cls.drop_parse(drop_elems)

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
    

