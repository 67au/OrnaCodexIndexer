import asyncio
from typing import Optional, Union, AsyncIterator

from httpx import AsyncClient, Timeout, Response

PLAYORNA_URL = 'https://playorna.com'


class Client:

    def __init__(self, **kwargs):
        self._client = AsyncClient(timeout=Timeout(300, connect=300))
        self.set_params(**kwargs)

    def set_params(self, **kwargs):
        self._client.params = self._client.params.merge(kwargs)

    async def fetch(self, path: str, data: dict = None, raw: bool = False) -> Union[Response, str]: # type: ignore
        r = await self._client.get(url=f'{PLAYORNA_URL}{path}', params=data)
        if raw:
            return r
        else:
            return r.text

    async def fetch_index(self, index_name: str, page: int = 1) -> Optional[str]:
        r: Response = await self.fetch(f'/codex/{index_name}/', data={'p': page}, raw=True) # type: ignore
        if r.status_code == 200:
            return r.text
        else:
            return None
    
    async def fetch_index_iter(self, index_name: str, start: int = 1, end: int = -1) -> AsyncIterator[str]:
        page = start
        while True:
            r = await self.fetch_index(index_name, page)
            if r is None:
                return
            yield r
            if page == end:
                return
            page += 1

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self):
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()