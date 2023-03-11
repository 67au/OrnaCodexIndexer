import re
from collections import defaultdict
from typing import Union

from lxml import etree

from .codex_types import CodexType

EFFECT_PATTERN = r'(?P<EFFECT>.+) \((?P<CHANCE>\d+%)\)'
effect_pattern = re.compile(EFFECT_PATTERN)
META_PATTERN = r'(?P<KEY>.+): (?P<VALUE>.+)'
meta_pattern = re.compile(META_PATTERN)

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
    def meta_parse_iter(cls, elems: list):
        for elem in elems:
            meta = elem.xpath("string()").strip()
            matches = meta_pattern.match(meta)
            if matches:
                yield [matches.group('KEY'), matches.group('VALUE')]
            else:
                yield meta

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

        description_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/pre[@class="codex-page-description"]')
        description = description_elems[0].text if description_elems else ''

        description_tag_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//div[@class="codex-page-description"]')

        meta_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//div[@class="codex-page-meta"]')
        meta = list((*cls.meta_parse_iter(description_tag_elems), *cls.meta_parse_iter(meta_elems)))

        stat_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/div[@class="codex-stats"]//div[@class="codex-stat"]')
        stat = list(cls.meta_parse_iter(stat_elems))

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
                stat=stat,
                drop=drop,
            )
    

