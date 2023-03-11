from lxml import etree


class IndexParser:

    @classmethod
    def parse_iter(cls, html: str):
        parser = etree.HTMLParser(encoding='utf-8')
        page = etree.HTML(html, parser=parser)
        entries_elems = page.xpath('/html/body/div[@class="codex"]/div[@class="codex-entries"]/a')
        for elem in entries_elems:
            yield {
                'name': elem.xpath('div[2]')[0].xpath('string()').strip(),
                'codex': elem.attrib['href'],
            }