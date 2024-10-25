import uuid

from pydantic import BaseModel
from typing import Optional


class Category(BaseModel):
    id: uuid.UUID = None
    name: str
    depth: int
    ancestor: Optional[str] = None
    parent: Optional[str] = None
    path: str
    url: str
    is_leaf: bool = False
