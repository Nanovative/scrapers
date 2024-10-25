import asyncio
import logging
import random
import uuid

from shared.models.cookie import Cookie, AmazonCookieRequest
from shared.models.enums import BrowserType
from shared.storages.cookie_set.base import CookieSetStorage
from shared.services.cookie import get_cookies

postcode_pool = [
    99501,  # Anchorage
    # 93311,  # Bakersfield
    # 71601,  # Pine Bluff
    # 11001,  # Floral Park
]


def get_new_postcode_pool():
    postcode_pool = [
        random.randint(99501, 99950),  # Alaska
        random.randint(35004, 36925),  # Alabama
        random.randint(71601, 72959),  # Arkansas
        random.randint(96799, 96799),  # Soamoa
        random.randint(85001, 86556),  # Arizona
        random.randint(90001, 96162),  # California
        random.randint(80001, 81658),  # Colorado
        random.randint(43001, 45999),  # Ohio
    ]
    return postcode_pool


get_random_us_postcode = lambda: postcode_pool[
    random.randint(0, len(postcode_pool) - 1)
]


class AmazonCookieSetPool:
    _instance = None

    def __new__(
        cls, storages: dict[BrowserType, CookieSetStorage] = None
    ) -> "AmazonCookieSetPool":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialize(storages)
        return cls._instance

    def initialize(
        self,
        storages: dict[BrowserType, CookieSetStorage] = None,
    ):
        self._browser_types: set[str] = set()
        self._pool: dict[BrowserType, CookieSetStorage] = dict()
        if storages:
            self._browser_types = {browser_type.value for browser_type in storages}
            self._pool: dict[BrowserType, CookieSetStorage] = storages

    @staticmethod
    def is_initialized() -> bool:
        return AmazonCookieSetPool._instance is not None

    async def clean(
        self,
        browser_type: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        if browser_type not in self._browser_types:
            return

        await self._pool[BrowserType(browser_type)].clean(coroutine_id, lock)

    async def pool_size(self, browser_type: str):
        if browser_type not in self._browser_types:
            return 0
        size = await self._pool[BrowserType(browser_type)].current_size()
        return size

    async def max_pool_size(self, browser_type: str):
        if browser_type not in self._browser_types:
            return 0
        size = self._pool[BrowserType(browser_type)].max_size()
        return size

    async def is_full(self, browser_type: str):
        if browser_type not in self._browser_types:
            return None

        return await self._pool[BrowserType(browser_type)].is_full()

    async def is_empty(self, browser_type: str):
        if browser_type not in self._browser_types:
            return None

        return await self._pool[BrowserType(browser_type)].is_empty()

    async def get(
        self,
        browser_type: str,
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        async with lock:
            if browser_type not in self._browser_types:
                return None

            await self.clean(browser_type, coroutine_id, None)

            cookie_set = await self._pool[BrowserType(browser_type)].get(
                coroutine_id, None
            )
            return cookie_set

    async def add(
        self,
        browser_type: str,
        postcode: int,
        location: str,
        cookies: list[Cookie],
        coroutine_id: uuid.UUID = None,
        lock: asyncio.Lock = None,
    ):
        if browser_type not in self._browser_types:
            return False

        success = await self._pool[BrowserType(browser_type)].add(
            postcode, location, cookies, coroutine_id, lock
        )

        return success


async def start_cleanup_task(
    pool: AmazonCookieSetPool,
    coroutine_id: uuid.UUID = uuid.uuid4(),
    lock: asyncio.Lock = None,
):
    for browser_type in pool._browser_types:
        old_pool_size = await pool.pool_size(browser_type)
        await pool.clean(browser_type, coroutine_id, lock)
        new_pool_size = await pool.pool_size(browser_type)
        logging.info(
            f"[coroutine_id={coroutine_id}]: Pool size before/after cleaning: [{old_pool_size}/{new_pool_size}]"
        )


async def start_add_task(
    pool: AmazonCookieSetPool,
    coroutine_id: uuid.UUID = uuid.uuid4(),
    lock: asyncio.Lock = None,
    is_independent_loop=True,
):
    async def helper():
        for browser_type in pool._browser_types:
            if await pool.is_full(browser_type):
                pool_size = await pool.pool_size(browser_type)
                logging.info(
                    f"[coroutine_id={coroutine_id}]: Cookie set pool is full ({pool_size})"
                )
                return
            postcode = get_random_us_postcode()
            body = AmazonCookieRequest(
                postcode=postcode,
                include_html=False,
                is_headless=True,
                max_timeout=15000,
                browser_type=browser_type,
            )
            resp = await get_cookies(body)
            cookies = resp["cookies"]
            location = resp["location"]

            logging.info(
                f"[coroutine_id={coroutine_id}]: Add task output: [message={resp['message']}, location={resp['location']}]"
            )

            if resp["message"] == "ok":
                old_pool_size = await pool.pool_size(browser_type)
                await pool.add(browser_type, postcode, location, cookies, lock)
                new_pool_size = await pool.pool_size(browser_type)
                logging.info(
                    f"[coroutine_id={coroutine_id}]: Pool size before/after adding: [{old_pool_size}/{new_pool_size}]"
                )

    if is_independent_loop:
        while True:
            await helper()
            await asyncio.sleep(1)

    else:
        await helper()
