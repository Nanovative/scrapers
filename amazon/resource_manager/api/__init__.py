import asyncio

from services.amazon_cookie_pool import AmazonCookieSetPool
from models.enums import BrowserType
from factories.storage_factory import storage_factory
from os import getenv

event_queue = asyncio.Queue(10)
event_loop_lock = asyncio.Lock()


async def get_cookie_set_pool():
    if not AmazonCookieSetPool._is_initialized():
        storages = await storage_factory(
            {
                BrowserType.firefox: {
                    "pool_args": {
                        "conn_str": getenv("POSTGRESQL_CONN_STR", None),
                        "max_conn": 4,
                        "max_cookie_set": 10,
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
