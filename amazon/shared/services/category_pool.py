import uuid
import asyncio
from shared.storages.category.base import CategoryStorage
from shared.models.category import Category
from typing import Optional


class CategoryPool:
    _instance = None

    def __new__(cls, storage: CategoryStorage = None) -> "CategoryPool":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialize(storage)
        return cls._instance

    def initialize(
        self,
        storage: CategoryStorage = None,
    ):
        self._pool: CategoryStorage = None
        if storage:
            self._pool = storage

    @staticmethod
    def is_initialized() -> bool:
        return CategoryPool._instance is not None

    async def replace(
        self,
        categories: list[Category],
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        return await self._pool.replace(categories, coroutine_id, lock)

    async def get_by_name(
        self,
        name: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> Optional[Category]:
        return await self._pool.get_by_name(name, coroutine_id, lock)

    async def get_by_depth(
        self,
        depth: int,
        strict: bool,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> tuple[list[Category], int]:
        return await self._pool.get_by_depth(depth, strict, coroutine_id, lock)

    async def get_by_ancestor(
        self,
        ancestor: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> list[Category]:
        return await self._pool.get_by_ancestor(ancestor, coroutine_id, lock)

    async def get_by_parent(
        self,
        parent: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> list[Category]:
        return await self._pool.get_by_parent(parent, coroutine_id, lock)

    async def get_by_leaf(
        self,
        is_leaf: bool,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> list[Category]:
        return await self._pool.get_by_leaf(is_leaf, coroutine_id, lock)

    async def get_by_ancestors_and_depth(
        self,
        ancestors: list[str],
        depth: int,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ) -> list[Category]:
        return await self._pool.get_by_ancestors_and_depth(
            ancestors, depth, coroutine_id, lock
        )
