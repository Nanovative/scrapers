import logging

from typing import Any
from shared.storages.cookie_set.base import CookieSetStorage
from shared.storages.proxy.base import ProxyStorage
from shared.storages.category.base import CategoryStorage
from shared.models.enums import BrowserType
from shared.config.impls import (
    COOKIE_SET_POOL_IMPLS,
    PROXY_POOL_IMPLS,
    CATEGORY_POOL_IMPLS,
)


async def cookie_set_storage_factory(
    cfg: dict[BrowserType, dict[str, Any]]
) -> dict[BrowserType, CookieSetStorage]:
    cookie_set_storages: dict[BrowserType, CookieSetStorage] = {}
    for browser_type, cookie_pool_cfg in cfg.items():
        pool_type: str = cookie_pool_cfg.get("pool_type")
        if not pool_type or pool_type not in COOKIE_SET_POOL_IMPLS:
            logging.info(f"Invalid pool type: {cookie_set_pool_impl}")
            return None

        pool_args: dict[str, Any] = cookie_pool_cfg.get("pool_args")
        if not pool_args:
            logging.info(f"Missing pool_args: {pool_type}")
            return None

        cookie_set_pool_impl = COOKIE_SET_POOL_IMPLS.get(pool_type)
        if not cookie_set_pool_impl or not issubclass(
            cookie_set_pool_impl, CookieSetStorage
        ):
            logging.info(f"Invalid pool implementation: {cookie_set_pool_impl}")
            return None

        cookie_set_storage = cookie_set_pool_impl(
            **pool_args, browser_type=browser_type
        )
        if callable(getattr(cookie_set_storage, "initialize", None)):
            await cookie_set_storage.initialize()

        cookie_set_storages[browser_type] = cookie_set_storage

    return cookie_set_storages


async def proxy_storage_factory(cfg: dict[str, Any]) -> ProxyStorage:
    proxy_storage: ProxyStorage = {}

    pool_type: str = cfg.get("pool_type")
    if not pool_type or pool_type not in PROXY_POOL_IMPLS:
        logging.info(f"Invalid pool type: {pool_type}")
        return None

    pool_args: dict[str, Any] = cfg.get("pool_args")
    if not pool_args:
        logging.info(f"Missing pool_args: {pool_type}")
        return None

    proxy_pool_impl = PROXY_POOL_IMPLS.get(pool_type)
    if not proxy_pool_impl or not issubclass(proxy_pool_impl, ProxyStorage):
        logging.info(f"Invalid pool implementation: {proxy_pool_impl}")
        return None

    proxy_storage = proxy_pool_impl(**pool_args)
    if callable(getattr(proxy_storage, "initialize", None)):
        await proxy_storage.initialize()

    return proxy_storage


async def category_storage_factory(cfg: dict[str, Any]) -> CategoryStorage:
    category_storage: CategoryStorage = {}

    pool_type: str = cfg.get("pool_type")
    if not pool_type or pool_type not in CATEGORY_POOL_IMPLS:
        logging.info(f"Invalid pool type: {pool_type}")
        return None

    pool_args: dict[str, Any] = cfg.get("pool_args")
    if not pool_args:
        logging.info(f"Missing pool_args: {pool_type}")
        return None

    category_pool_impl = CATEGORY_POOL_IMPLS.get(pool_type)
    if not category_pool_impl or not issubclass(category_pool_impl, CategoryStorage):
        logging.info(f"Invalid pool implementation: {category_pool_impl}")
        return None

    category_storage = category_pool_impl(**pool_args)
    if callable(getattr(category_storage, "initialize", None)):
        await category_storage.initialize()

    return category_storage
