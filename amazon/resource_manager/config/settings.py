from storages.cookie_set.base import CookieSetStorage
from storages.proxy.base import ProxyStorage

from storages.cookie_set.linked_list import LinkedListCookieSetStorage
from storages.cookie_set.postgresql import PostgreSQLCookieSetStorage

from storages.proxy.postgresql import PostgreSQLProxyStorage

COOKIE_SET_POOL_IMPLS: dict[str, CookieSetStorage] = {
    "linked_list": LinkedListCookieSetStorage,
    "postgresql": PostgreSQLCookieSetStorage,
}

PROXY_POOL_IMPLS: dict[str, ProxyStorage] = {
    "postgresql": PostgreSQLProxyStorage,
}
