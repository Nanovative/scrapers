import asyncio


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
