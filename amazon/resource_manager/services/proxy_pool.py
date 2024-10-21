import json
import aiofiles


class ProxyPool:
    def __init__(self):
        self.proxies: list[str] = []
        self.current_index: int = 0

    def __len__(self):
        return len(self.proxies)

    @staticmethod
    async def load_proxies(file_path: str) -> list[str]:
        async with aiofiles.open(file_path, "r") as f:
            # proxy file is an array-of-str JSON file
            content: list[str] = json.load(f)
            return content

    async def init_proxy(self, proxy_file: str):
        self.proxies = await self.load_proxies(proxy_file)

    async def get_proxy(self) -> tuple[str, dict]:
        proxy_str = self.proxies[self.current_index % len(self.proxies)]
        self.current_index += 1

        proxy_parts = proxy_str.split(":")
        host, port, username, password = proxy_parts[:4]
        proxy_url = f"http://{username}:{password}@{host}:{port}"

        headers = {}
        for part in proxy_parts[4:]:
            if part.startswith("country-"):
                headers["X-Country"] = part.split("-")[1]
            elif part.startswith("session-"):
                headers["X-Session"] = part.split("-")[1]
            elif part.startswith("lifetime-"):
                headers["X-Lifetime"] = part.split("-")[1]
            elif part.startswith("state-"):
                headers["X-State"] = part.split("-")[1]
            elif part.startswith("streaming-"):
                headers["X-Streaming"] = part.split("-")[1]

        return proxy_url, headers

    async def rotate_proxy(self):
        self.current_index = (self.current_index + 1) % len(self.proxies)

    async def pool_size(self):
        return len(self.proxies)

    async def is_empty(self):
        return len(self.proxies) == 0


# Example usage:
async def example_usage():
    proxy_pool = ProxyPool("setup/proxies.json")

    # Get a proxy
    proxy_url, headers = proxy_pool.get_proxy()
    print(f"Proxy URL: {proxy_url}, Headers: {headers}")

    # Rotate proxy
    await proxy_pool.rotate_proxy()
    proxy_url, headers = proxy_pool.get_proxy()
    print(f"Rotated Proxy URL: {proxy_url}, Headers: {headers}")
