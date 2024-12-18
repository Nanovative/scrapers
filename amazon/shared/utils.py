import json
import random
import asyncio

from filelock import FileLock
from shared.services.cookie_set_pool import AmazonCookieSetPool
from shared.services.proxy_pool import ProxyPool
from shared.services.category_pool import CategoryPool
from shared.models.enums import BrowserType
from shared.factories.storage_factory import (
    cookie_set_storage_factory,
    proxy_storage_factory,
    category_storage_factory,
)
from os import getenv
from typing import Any

event_queue = asyncio.Queue(5)
event_loop_lock = asyncio.Lock()

class AsyncSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = asyncio.Lock()

    async def set(self, key, value):
        async with self._lock:
            self._dict[key] = value

    async def get(self, key):
        async with self._lock:
            return self._dict.get(key)

    async def delete(self, key):
        async with self._lock:
            if key in self._dict:
                del self._dict[key]

    async def items(self):
        async with self._lock:
            return list(self._dict.items())


def save_json_file(file_path: str, data: Any):
    lock_path = f"{file_path}.lock"
    with FileLock(lock_path):
        with open(file_path, "w") as file:
            json.dump(data, file, indent=2)


def load_json_file(file_path: str):
    lock_path = f"{file_path}.lock"
    with FileLock(lock_path):
        with open(file_path, "r") as file:
            return json.load(file)


def run_event_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def sleep_randomly(start: float, end: float):
    delay = random.uniform(start, end)
    await asyncio.sleep(delay)


async def get_cookie_set_pool():
    if not AmazonCookieSetPool.is_initialized():
        storages = await cookie_set_storage_factory(
            {
                BrowserType.firefox: {
                    "pool_args": {
                        "conn_str": getenv("POSTGRESQL_CONN_STR", None),
                        "max_conn": 2,
                        "max_cookie_set": 40,
                    },
                    "pool_type": "postgresql",
                }
            }
        )
        assert storages is not None
        cookie_set_pool = AmazonCookieSetPool(storages)
    else:
        cookie_set_pool = AmazonCookieSetPool()

    assert (
        cookie_set_pool is not None
    ), "Can't initialize cookie_set_pool (cookie_set_pool = None)"
    return cookie_set_pool


async def get_proxy_pool():
    if not ProxyPool.is_initialized():
        storage = await proxy_storage_factory(
            {
                "pool_args": {
                    "conn_str": getenv("POSTGRESQL_CONN_STR", None),
                    "max_conn": 2,
                },
                "pool_type": "postgresql",
            }
        )
        assert storage is not None
        proxy_pool = ProxyPool(storage)
    else:
        proxy_pool = ProxyPool()

    assert proxy_pool is not None, "Can't initialize proxy_pool (proxy_pool = None)"
    return proxy_pool


async def get_category_pool():
    if not CategoryPool.is_initialized():
        storage = await category_storage_factory(
            {
                "pool_args": {
                    "conn_str": getenv("POSTGRESQL_CONN_STR", None),
                    "max_conn": 2,
                },
                "pool_type": "postgresql",
            }
        )
        assert storage is not None
        category_pool = CategoryPool(storage)
    else:
        category_pool = CategoryPool()

    assert (
        category_pool is not None
    ), "Can't initialize category_pool (category_pool = None)"
    return category_pool
