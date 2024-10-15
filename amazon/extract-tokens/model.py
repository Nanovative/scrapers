import asyncio

from enum import Enum
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import TypeVar, Generic, Optional

T = TypeVar("T")
event_loop_lock = asyncio.Lock()


class BrowserType(str, Enum):
    firefox = "firefox"
    chromium = "chromium"


class AmazonCookieRequest(BaseModel):
    postcode: int = None
    include_html: bool = None
    is_headless: bool = None
    max_timeout: int = 15000
    browser_type: BrowserType = BrowserType.firefox
    do_fetch_pool: bool = True


class Cookie(BaseModel):
    name: str
    value: str
    domain: str
    path: str
    expires: int
    httpOnly: bool
    secure: bool
    sameSite: str = None


class AmazonCookieSet(BaseModel):
    postcode: int = None
    cookies: list[Cookie]
    location: str
    expires: datetime = datetime.now() + timedelta(hours=1)


class Node(Generic[T]):
    def __init__(self, value: T):
        self.value: T = value
        self.prev: Optional[Node[T]] = None
        self.next: Optional[Node[T]] = None


class LinkedListDeque(Generic[T]):
    def __init__(self, max_len: int):
        self._max_len = max_len
        self._size = 0
        self._head: Optional[Node[T]] = None
        self._tail: Optional[Node[T]] = None

    def __len__(self) -> int:
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


class AmazonCookieSetDeque:
    def __init__(self, /, max_len=100) -> None:
        self.queue = LinkedListDeque[AmazonCookieSet](max_len=max_len)
        self.queue_size = max_len

    def __len__(self):
        return len(self.queue)
    
    def max_size(self):
        return self.queue_size

    async def add(
        self,
        postcode: int,
        location: str,
        cookies: list[Cookie],
        lock: asyncio.Lock = None,
    ):
        async def helper():
            item = AmazonCookieSet(
                postcode=postcode,
                cookies=cookies,
                location=location,
            )
            return self.queue.prepend(item)

        if lock is None:
            return await helper()
        async with lock:
            return await helper()

    async def pop(self, lock: asyncio.Lock = None):
        async def helper():
            return self.queue.pop()

        if lock is None:
            return await helper()
        async with lock:
            return await helper()

    async def clean(self, lock: asyncio.Lock = None):
        async def helper():
            while not self.queue.is_empty():
                tail = self.queue.tail()
                if tail is None:
                    return
                if tail.expires > datetime.now():
                    return
                self.queue.pop()

        if lock is None:
            return await helper()
        async with lock:
            return await helper()

    async def is_full(self):
        return self.queue.is_full()

    async def is_empty(self):
        return self.queue.is_empty()


class AmazonCookieSetPool:
    def __init__(self, pool_size: int = 100):
        self._pool_size = pool_size
        self._cookie_set_expire = timedelta(hours=1)
        self._pool_weight = {
            BrowserType.firefox.value: 1.00,
        }
        self._pool: dict[BrowserType, AmazonCookieSetDeque] = {
            browser_type: AmazonCookieSetDeque(
                int(self._pool_size * browser_pool_weight)
            )
            for browser_type, browser_pool_weight in self._pool_weight.items()
        }

    async def clean(self, browser_type: str, lock: asyncio.Lock = None):
        if browser_type not in self._pool_weight:
            return

        old_pool_size = await self.pool_size(browser_type)
        await self._pool[browser_type].clean(lock)
        new_pool_size = await self.pool_size(browser_type)
        print(f"pool size before/after cleaning: [{old_pool_size}/{new_pool_size}]")

    async def pool_size(self, browser_type: str):
        if browser_type not in self._pool_weight:
            return 0
        size = len(self._pool[browser_type])
        return size
    
    async def max_pool_size(self, browser_type: str):
        if browser_type not in self._pool_weight:
            return 0
        size = self._pool[browser_type].queue_size
        return size

    async def is_full(self, browser_type: str):
        if browser_type not in self._pool_weight:
            return None

        return await self._pool[browser_type].is_full()

    async def is_empty(self, browser_type: str):
        if browser_type not in self._pool_weight:
            return None

        return await self._pool[browser_type].is_empty()

    async def get(self, browser_type: str, lock: asyncio.Lock = None):
        async with lock:
            if browser_type not in self._pool_weight:
                return None

            await self.clean(browser_type, None)

            old_pool_size = await self.pool_size(browser_type)
            cookie_set = await self._pool[browser_type].pop(None)
            new_pool_size = await self.pool_size(browser_type)
            print(f"pool size before/after getting: [{old_pool_size}/{new_pool_size}]")
            return cookie_set

    async def add(
        self,
        browser_type: str,
        postcode: int,
        location: str,
        cookies: list[Cookie],
        lock: asyncio.Lock = None,
    ):
        print("Adding")
        if browser_type not in self._pool_weight:
            return False

        old_pool_size = await self.pool_size(browser_type)
        success = await self._pool[browser_type].add(postcode, location, cookies, lock)
        new_pool_size = await self.pool_size(browser_type)
        print(f"pool size before/after adding: [{old_pool_size}/{new_pool_size}]")

        return success


pool = AmazonCookieSetPool(300)
