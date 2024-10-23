import uuid

from pydantic import BaseModel
from datetime import datetime
from models.enums import BrowserType


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
    usable_times: int = 5
    last_used: datetime = None


class AmazonCookieRequest(BaseModel):
    request_id: uuid.UUID = None
    postcode: int = None
    include_html: bool = None
    is_headless: bool = None
    max_timeout: int = 15000
    browser_type: BrowserType = BrowserType.firefox
    do_fetch_pool: bool = True
