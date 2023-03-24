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
    
    @classmethod
    def parse_codex_index(cls, html: str):
        parser = etree.HTMLParser(encoding='utf-8')
        page = etree.HTML(html, parser=parser)
        elems = page.xpath('/html/body/div[@class="wraps"]/div[@class="page"]/a[@class="codex-link"]')
        index = {elem.attrib['href'].strip('/').split('/')[-1]:elem.xpath('string()').strip()[:-2] for elem in elems}
        lcp = ""
        for tmp in zip(*index.values()):
            if len(set(tmp)) == 1:
                lcp += tmp[0]
            else:
                break
        return {k:v[len(lcp):] for k,v in index.items()}