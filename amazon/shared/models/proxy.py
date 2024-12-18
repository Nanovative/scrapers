import uuid

from pydantic import BaseModel
from datetime import datetime
from shared.models.enums import ProxyType


class Proxy(BaseModel):
    id: uuid.UUID = None
    provider: str
    proxies: list[str]
    last_used: datetime = None

class ProxyConf(BaseModel):
    server: str
    username: str
    password: str


class ProxyRequest(BaseModel):
    request_id: uuid.UUID = None
    provider: str
    proxy_type: ProxyType = ProxyType.dynamic
    tag: str = None
    proxies: list[str] = []
