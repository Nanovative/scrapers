from abc import ABC, abstractmethod
from models.cookie import Cookie, AmazonCookieSet
import asyncio
from typing import Optional


class CookieSetStorage(ABC):
    @abstractmethod
    async def add(
        self,
        postcode: int,
        location: str,
        cookies: list[Cookie],
        lock: asyncio.Lock = None,
    ) -> bool:
        pass

    @abstractmethod
    async def get(self, lock: asyncio.Lock = None) -> Optional[AmazonCookieSet]:
        pass

    @abstractmethod
    async def clean(self, lock: asyncio.Lock = None) -> None:
        pass

    @abstractmethod
    async def is_full(self) -> bool:
        pass

    @abstractmethod
    async def is_empty(self) -> bool:
        pass

    @abstractmethod
    async def current_size(self) -> int:
        pass

    @abstractmethod
    def max_size(self) -> int:
        pass
