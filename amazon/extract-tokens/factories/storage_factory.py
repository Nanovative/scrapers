from typing import Any
from storages.cookie_set.base import CookieSetStorage
from models.enums import BrowserType
from config.settings import POOL_IMPLEMENTATIONS


async def storage_factory(
    cfg: dict[BrowserType, dict[str, Any]]
) -> dict[BrowserType, CookieSetStorage]:
    cookie_set_storages: dict[BrowserType, CookieSetStorage] = {}
    for browser_type, cookie_pool_cfg in cfg.items():
        pool_type: str = cookie_pool_cfg.get("pool_type")
        if not pool_type or pool_type not in POOL_IMPLEMENTATIONS:
            print("Invalid pool type:", pool_type)
            return None

        pool_args: dict[str, Any] = cookie_pool_cfg.get("pool_args")
        if not pool_args:
            print("Missing pool_args:", pool_type)
            return None

        pool_impl = POOL_IMPLEMENTATIONS.get(pool_type)
        if not pool_impl or not issubclass(pool_impl, CookieSetStorage):
            print("Invalid pool implementation", pool_impl)
            return None

        cookie_set_storage = pool_impl(**pool_args)
        if callable(getattr(cookie_set_storage, "initialize", None)):
            await cookie_set_storage.initialize()

        cookie_set_storages[browser_type] = cookie_set_storage

    return cookie_set_storages
