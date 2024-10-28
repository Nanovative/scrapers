import asyncio

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

event_queue = asyncio.Queue(5)
event_loop_lock = asyncio.Lock()


async def get_cookie_set_pool():
    if not AmazonCookieSetPool.is_initialized():
        storages = await cookie_set_storage_factory(
            {
                BrowserType.firefox: {
                    "pool_args": {
                        "conn_str": getenv("POSTGRESQL_CONN_STR", None),
                        "max_conn": 2,
                        "max_cookie_set": 20,
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
