import uuid
import asyncio

from shared.storages.proxy.base import ProxyStorage


class ProxyPool:
    _instance = None

    def __new__(cls, storage: ProxyStorage = None) -> "ProxyPool":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialize(storage)
        return cls._instance

    def initialize(
        self,
        storage: ProxyStorage = None,
    ):
        self._pool: ProxyStorage = None
        if storage:
            self._pool = storage

    @staticmethod
    def is_initialized() -> bool:
        return ProxyPool._instance is not None

    async def replace(
        self,
        proxies: list[str],
        proxy_type: str,
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        return await self._pool.replace(
            proxies,
            proxy_type=proxy_type,
            tag=tag,
            provider=provider,
            coroutine_id=coroutine_id,
            lock=lock,
        )

    async def pool_size(
        self, proxy_type: str, tag: str = None, provider: str = "iproyal"
    ):
        size = await self._pool.current_size(
            tag=tag, proxy_type=proxy_type, provider=provider
        )
        return size

    async def is_empty(
        self, proxy_type: str, tag: str = None, provider: str = "iproyal"
    ):
        return await self._pool.is_empty(
            tag=tag, proxy_type=proxy_type, provider=provider
        )

    async def rotate(
        self,
        proxy_type: str,
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        cookie_set = await self._pool.rotate(
            tag=tag,
            proxy_type=proxy_type,
            provider=provider,
            coroutine_id=coroutine_id,
            lock=lock,
        )
        return cookie_set
