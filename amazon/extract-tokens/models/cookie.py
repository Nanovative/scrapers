from pydantic import BaseModel
from datetime import datetime, timedelta
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
    postcode: int = None
    cookies: list[Cookie]
    location: str
    expires: datetime = datetime.now() + timedelta(days=3)
    usable_times: int = 5
    last_used: datetime = None


class AmazonCookieRequest(BaseModel):
    postcode: int = None
    include_html: bool = None
    is_headless: bool = None
    max_timeout: int = 15000
    browser_type: BrowserType = BrowserType.firefox
    do_fetch_pool: bool = True
