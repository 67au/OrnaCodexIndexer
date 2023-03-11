import asyncio
import json

from httpx import AsyncClient, Timeout


ORNA_GUIDE_URL = 'https://orna.guide'
ORNA_GUIDE_API_URL = 'https://orna.guide/api/v1'


class Client:

    def __init__(self):
        self._client = AsyncClient(timeout=Timeout(300, read=600))

    async def fetch(self, interface: str, data: dict) -> dict:
        r = await self._client.post(
            url=f'{ORNA_GUIDE_API_URL}/{interface}',
            content=json.dumps(data),
        )
        return r.json()

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()