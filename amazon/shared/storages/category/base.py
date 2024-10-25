import uuid
import asyncio
from abc import ABC, abstractmethod
from typing import List, Optional

from shared.models.category import Category


class CategoryStorage(ABC):

    @abstractmethod
    async def replace(
        self,
        categories: List[Category],
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> bool:
        pass

    @abstractmethod
    async def get_by_name(
        self,
        name: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> Optional[Category]:
        pass

    @abstractmethod
    async def get_by_depth(
        self,
        depth: int,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> List[Category]:
        pass

    @abstractmethod
    async def get_by_ancestor(
        self,
        ancestor: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> List[Category]:
        pass

    @abstractmethod
    async def get_by_parent(
        self,
        parent: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> List[Category]:
        pass

    @abstractmethod
    async def get_by_leaf(
        self,
        is_leaf: bool,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> List[Category]:
        pass

    @abstractmethod
    async def get_by_ancestors_and_depth(
        self,
        ancestors: List[str],
        depth: int,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> List[Category]:
        pass
