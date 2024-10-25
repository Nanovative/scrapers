from shared.storages.category.base import CategoryStorage
from shared.storages.cookie_set.base import CookieSetStorage
from shared.storages.proxy.base import ProxyStorage

from shared.storages.cookie_set.linked_list import LinkedListCookieSetStorage
from shared.storages.cookie_set.postgresql import PostgreSQLCookieSetStorage

from shared.storages.proxy.postgresql import PostgreSQLProxyStorage

from shared.storages.category.postgresql import PostgreSQLCategoryStorage

COOKIE_SET_POOL_IMPLS: dict[str, CookieSetStorage] = {
    "postgresql": PostgreSQLCookieSetStorage,
}

PROXY_POOL_IMPLS: dict[str, ProxyStorage] = {
    "postgresql": PostgreSQLProxyStorage,
}

CATEGORY_POOL_IMPLS: dict[str, CategoryStorage] = {
    "postgresql": PostgreSQLCategoryStorage,
}
