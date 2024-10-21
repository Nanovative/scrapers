import uuid

from pydantic import BaseModel
from datetime import datetime
from models.enums import BrowserType


class Proxy(BaseModel):
    id: uuid.UUID = None
    provider: str
    proxies: list[str]
    last_used: datetime = None


class ProxyRequest(BaseModel):
    request_id: uuid.UUID = None
    provider: str
    tag: str = None
    proxies: list[str] = []
