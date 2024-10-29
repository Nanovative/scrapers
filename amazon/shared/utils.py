import json
import random
import asyncio

from filelock import FileLock
from typing import Any


class AsyncSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = asyncio.Lock()

    async def set(self, key, value):
        async with self._lock:
            self._dict[key] = value

    async def get(self, key):
        async with self._lock:
            return self._dict.get(key)

    async def delete(self, key):
        async with self._lock:
            if key in self._dict:
                del self._dict[key]

    async def items(self):
        async with self._lock:
            return list(self._dict.items())


def save_json_file(file_path: str, data: Any):
    lock_path = f"{file_path}.lock"
    with FileLock(lock_path):
        with open(file_path, "w") as file:
            json.dump(data, file, indent=2)


def load_json_file(file_path: str):
    lock_path = f"{file_path}.lock"
    with FileLock(lock_path):
        with open(file_path, "r") as file:
            return json.load(file)


def run_event_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def sleep_randomly(start: float, end: float):
    delay = random.uniform(start, end)
    await asyncio.sleep(delay)
