from models.enums import BrowserType
from storages.cookie_set.base import CookieSetStorage
from storages.cookie_set.linked_list import LinkedListCookieSetStorage
from storages.cookie_set.postgresql import PostgreSQLCookieSetStorage

POOL_IMPLEMENTATIONS: dict[str, CookieSetStorage] = {
    "linked_list": LinkedListCookieSetStorage,
    "postgresql": PostgreSQLCookieSetStorage,
}
