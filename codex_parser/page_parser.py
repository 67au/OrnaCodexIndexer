import re
from collections import defaultdict
from typing import Iterator, Tuple, Union

from lxml import etree

from .codex_types import CodexType

SPLIT_PATTERN = r':|：'
STRIP_PATTERN = ''.join(SPLIT_PATTERN)
EFFECT_PATTERN = r'(?P<EFFECT>.+) \((?P<CHANCE>\d+%)\)'
effect_pattern = re.compile(pattern=EFFECT_PATTERN) # type: ignore
KV_PATTERN = rf'(?P<KEY>.+)({SPLIT_PATTERN}) (?P<VALUE>.+)'
kv_pattern = re.compile(pattern=KV_PATTERN) # type: ignore

class PageParser:

    @classmethod
    def drop_parse(cls, elems: list) -> list:
        drop = []
        data = []
        for elem in elems:
            if elem.tag == 'h4':
                drop_header = elem.text.strip(STRIP_PATTERN)
                data = []
                drop.append({'name': drop_header, 'base': data})
            elif elem.attrib.get('class') == 'drop':
                drop_str = elem.xpath("string()").strip() 
                icon = elem.xpath('img')[0].attrib.get('src')[31:]
                ability = elem.xpath('../div[@class="emph"]')
                matches = effect_pattern.match(drop_str)
                if elem.tag == 'a':
                    data.append({
                        'codex': elem.attrib.get('href'), 
                        'name': drop_str,
                        'icon': icon,
                    })
                elif matches:
                    data.append({
                        'name':matches.group('EFFECT'), 
                        'chance': matches.group('CHANCE'),
                        'icon': icon,
                    })
                elif ability:
                    data.append({
                        'name': drop_str, 
                        'icon': icon,
                        'ability': ability[0].xpath("string()").strip(),
                    })
                else:
                    data.append({'name': drop_str, 'icon': icon})
        return drop
    

    @classmethod
    def kv_parse_iter(cls, elems: Iterator):
        for elem in elems:
            kv = elem.xpath("string()").strip()
            matches = kv_pattern.match(kv)
            if matches:
                value = matches.group('VALUE')
                # fix event sort
                if elem.xpath('contains(@class, "codex-page-description-highlight")'):
                    value = ' / '.join(sorted(i.strip() for i in value.split('/')))
                yield {'name': matches.group('KEY'), 'base': value}
            else:
                yield {'name': kv}
    
    @classmethod
    def meta_parse_iter(cls, elems: list):
        for elem in elems:
            span = elem.xpath('span')
            if len(span) > 0 and span[0].xpath('boolean(@class="exotic")'):
                yield {'name': 'exotic', 'base': span[0].xpath("string()").strip()}
                continue
            meta = elem.xpath("string()").strip()
            matches = kv_pattern.match(meta)
            if matches:
                yield {'name': matches.group('KEY'), 'base': matches.group('VALUE')}
            else:
                yield {'name': meta}

    @classmethod
    def stat_parse_iter(cls, elems: list):
        for elem in elems:
            stats = elem.attrib.get('class').split()
            if len(stats) > 1:
                yield {'name': elem.xpath("string()").strip(), 'element': stats[1], }
                continue
            stat = elem.xpath("string()").strip()
            matches = kv_pattern.match(stat)
            if matches:
                yield {'name': matches.group('KEY'), 'base': matches.group('VALUE')}
            else:
                yield {'name': stat}

    @classmethod
    def description_parse(cls, pre_elems: list, div_elems: list, codex_type: str) -> Tuple[str, list, dict]:
        description = pre_elems[0].xpath("string()").strip() if len(pre_elems) > 0 else ''
        meta_extra = []
        offhand = {}
        if codex_type in {'items'} and len(div_elems) > 0:
            ability = re.split(SPLIT_PATTERN, div_elems[0].xpath("preceding-sibling::div[1]")[0].xpath("string()").strip())
            offhand = {
                'name': ability[0],
                'base': [{
                    'name': ability[1].strip(),
                    'ability': div_elems[0].xpath("string()").strip(),
                }]
            }
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
        tag = [{'name': i['name'][2:]} for i in cls.kv_parse_iter(tag_elems)]

        stat_elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/div[@class="codex-stats"]//div[contains(@class,"codex-stat")]')
        stat = list(cls.stat_parse_iter(stat_elems))

        drop_elems = page.xpath(
            '/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]/h4'
            '|/html/body/div[@class="wraps"]/div[@class="page"]/div[@class="codex-page"]//*[@class="drop"]'
        )
        drop = cls.drop_parse(drop_elems)
        if offhand:
            drop.append(offhand)

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
    

