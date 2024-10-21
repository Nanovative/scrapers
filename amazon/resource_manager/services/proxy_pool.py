import uuid
import asyncio

from storages.proxy.base import ProxyStorage
from storages.proxy.postgresql import PostgreSQLProxyStorage


class ProxyPool:
    _instance = None

    def __new__(cls, storage: ProxyStorage = None) -> "ProxyPool":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize(storage)
        return cls._instance

    def _initialize(
        self,
        storage: ProxyStorage = None,
    ):
        self._pool: ProxyStorage = None
        if storage:
            self._pool = storage

    @staticmethod
    def _is_initialized() -> bool:
        return ProxyPool._instance is not None

    async def replace(
        self,
        proxies: list[str],
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        return await self._pool.replace(proxies, tag, provider, coroutine_id, lock)

    async def pool_size(self, tag: str = None, provider: str = "iproyal"):
        size = await self._pool.current_size(tag, provider)
        return size

    async def is_empty(self, tag: str = None, provider: str = "iproyal"):
        return await self._pool.is_empty(tag, provider)

    async def rotate(
        self,
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        cookie_set = await self._pool.rotate(tag, provider, coroutine_id, lock)
        return cookie_set
