import uuid

from typing import Optional
from pydantic import BaseModel
from datetime import datetime
from shared.models.enums import BrowserType
from shared.models.proxy import ProxyConf

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
    id: uuid.UUID = None
    postcode: int = None
    cookies: list[Cookie]
    location: str
    expires: datetime
    usable_times: int = 10000
    last_used: datetime = None


class AmazonCookieRequest(BaseModel):
    request_id: uuid.UUID = None
    postcode: int = None
    include_html: bool = None
    is_headless: bool = None
    max_timeout: int = 15000
    browser_type: BrowserType = BrowserType.firefox
    do_fetch_pool: bool = True
    proxy_conf: Optional[ProxyConf] = None
