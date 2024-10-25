import uuid
import asyncio

from datetime import datetime, timedelta
from typing import TypeVar, Generic, Optional
from shared.models.cookie import Cookie, AmazonCookieSet
from shared.storages.cookie_set.base import CookieSetStorage

T = TypeVar("T")


class Node(Generic[T]):
    def __init__(self, value: T):
        self.value: T = value
        self.prev: Optional[Node[T]] = None
        self.next: Optional[Node[T]] = None


class LinkedListQueue(Generic[T]):
    def __init__(self, max_len: int):
        self._max_len = max_len
        self._size = 0
        self._head: Optional[Node[T]] = None
        self._tail: Optional[Node[T]] = None

    async def current_size(self) -> int:
        return self._size

    def max_len(self) -> int:
        return self._max_len

    def append(self, item: T) -> bool:
        if self.is_full():
            return False

        new_node = Node(item)

        if self.is_empty():
            self._head = self._tail = new_node
        else:
            assert self._tail is not None
            self._tail.next = new_node
            new_node.prev = self._tail
            self._tail = new_node

        self._size += 1
        return True

    def prepend(self, item: T) -> bool:
        if self.is_full():
            return False

        new_node = Node(item)

        if self.is_empty():
            self._head = self._tail = new_node
        else:
            assert self._head is not None
            self._head.prev = new_node
            new_node.next = self._head
            self._head = new_node

        self._size += 1
        return True

    def pop(self) -> Optional[T]:
        if self.is_empty():
            return None

        assert self._tail is not None
        value = self._tail.value
        if self._head == self._tail:
            self._head = self._tail = None
        else:
            self._tail = self._tail.prev
            self._tail.next = None

        self._size -= 1
        return value

    def head(self) -> Optional[T]:
        if self.is_empty():
            return None
        assert self._head is not None
        return self._head.value

    def tail(self) -> Optional[T]:
        if self.is_empty():
            return None
        assert self._tail is not None
        return self._tail.value

    def is_empty(self) -> bool:
        return self._size == 0

    def is_full(self) -> bool:
        return self._size >= self._max_len


class LinkedListCookieSetStorage(CookieSetStorage):
    def __init__(self, /, max_cookie_set=100, **kwargs) -> None:
        self.queue = LinkedListQueue[AmazonCookieSet](max_len=max_cookie_set)
        self.queue_size = max_cookie_set

    def max_size(self):
        return self.queue_size

    async def current_size(self) -> int:
        return await self.queue.current_size()

    async def _add(self, postcode: int, location: str, cookies: list[Cookie]) -> bool:
        item = AmazonCookieSet(
            id=uuid.uuid4(),
            postcode=postcode,
            cookies=cookies,
            location=location,
            expires=datetime.now() + timedelta(days=3),
        )
        return self.queue.prepend(item)

    async def _clean(self) -> None:
        while not self.queue.is_empty():
            tail = self.queue.tail()
            if tail is None:
                return
            if tail.expires > datetime.now():
                return
            self.queue.pop()

    async def _pop(self, lock: asyncio.Lock = None):
        async def helper():
            return self.queue.pop()

        if lock is None:
            return await helper()
        async with lock:
            return await helper()

    async def _get(self):
        return self.queue.pop()

    async def add(
        self,
        postcode: int,
        location: str,
        cookies: list[Cookie],
        lock: asyncio.Lock = None,
    ) -> bool:
        if lock is None:
            return await self._add(postcode, location, cookies)
        async with lock:
            return await self._add(postcode, location, cookies)

    async def clean(self, lock: asyncio.Lock = None) -> None:
        if lock is None:
            return await self._clean()
        async with lock:
            return await self._clean()

    async def get(self, lock: asyncio.Lock = None) -> AmazonCookieSet | None:
        if lock is None:
            return await self._get()
        async with lock:
            return await self._get()

    async def is_full(self) -> bool:
        return self.queue.is_full()

    async def is_empty(self) -> bool:
        return self.queue.is_empty()
