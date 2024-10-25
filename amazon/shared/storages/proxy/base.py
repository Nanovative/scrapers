import uuid
import asyncio

from abc import ABC, abstractmethod
from typing import Optional
from shared.models.proxy import Proxy


class ProxyStorage(ABC):

    @abstractmethod
    async def replace(
        self,
        proxies: list[str],
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> bool:
        pass

    @abstractmethod
    async def get_tags(self, lock: asyncio.Lock = None) -> list[str]:
        pass

    @abstractmethod
    async def rotate(
        self,
        tag: str = None,
        provider: str = "iproyal",
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> Optional[Proxy]:
        pass

    @abstractmethod
    async def current_size(self, tag: str = None, provider: str = "iproyal") -> int:
        pass

    @abstractmethod
    async def is_empty(self, tag: str = None, provider: str = "iproyal") -> bool:
        pass
