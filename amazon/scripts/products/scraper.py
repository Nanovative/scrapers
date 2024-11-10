import __init__
from __init__ import DEFAULT_DATA_DIR, DEFAULT_OUT_DIR

import os
import json
import asyncio
import logging
import aiofiles
import argparse
import curl_cffi
import urllib.parse as urlparser

from typing import Optional
from config import base_headers
from shared.models.proxy import Proxy
from shared.utils import sleep_randomly
from shared.models.category import Category
from curl_cffi.requests import AsyncSession

API_URL = os.getenv("API_URL", "http://localhost:8000")


async def get_categories(depth: int = 2, strict: bool = True):
    async with AsyncSession(http_version=curl_cffi.CurlHttpVersion.V1_1) as session:
        resp = await session.get(
            f"{API_URL}/category/get_by_depth",
            params={"depth": depth, "strict": strict},
        )
        data = resp.json()

        categories: list[Category] = [
            Category(**category) for category in data["categories"]
        ]
        count: int = data["count"]
        return (categories, count), resp.status_code


async def get_cookies():
    async with AsyncSession(http_version=curl_cffi.CurlHttpVersion.V1_1) as session:
        resp = await session.post(
            f"{API_URL}/cookie/fetch",
            json={
                "browser_type": "firefox",
                "do_fetch_pool": True,
            },
        )
        data = resp.json()

        cookie_dict: dict[str, str] = {
            cookie["name"]: cookie["value"] for cookie in data["cookies"]
        }
        postcode: int = data["postcode"]

        return (cookie_dict, postcode), resp.status_code


async def get_proxy():
    async with AsyncSession(http_version=curl_cffi.CurlHttpVersion.V1_1) as session:
        resp = await session.post(
            f"{API_URL}/proxy/rotate",
            json={"provider": "iproyal", "tag": "general", "proxy_type": "dynamic"},
        )
        data = resp.json()

        proxies: list[Proxy] = [
            Proxy(**{k: v for k, v in proxy.items() if k != "id"})
            for proxy in data["proxies"]
        ]

        proxy: Optional[Proxy] = proxies[0] if len(proxies) else None

        return proxy, resp.status_code


def preprocess_url_parts(category_url: str):
    # To be retained: rh, fs, i, ref, page
    original_qs = category_url.replace("https://amazon.com/s?", "")
    original_qs = {
        k: v[0]
        for k, v in urlparser.parse_qs(original_qs).items()
        if k in {"rh", "fs", "i"}
    }
    base_url = "https://www.amazon.com/s/query"
    return base_url, original_qs


def build_product_url(url: str, qs: dict[str, str], page: int):
    final_qs = {**qs, "page": page, "ref": f"sr_pg_{page}"}
    full_path = f"{url}?{urlparser.urlencode(final_qs)}"
    return full_path


def parse_proxy_str(proxy_str: Optional[str]):
    if not proxy_str:
        return None, {}

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


async def process_category_page(
    category: Category,
    url: str,
    qs: dict[str, str],
    page: int,
    cookies: dict[str, str],
    headers: dict[str, str],
    proxies: dict[str, str],
    should_stop: asyncio.Event,
    override_exist: bool = True,
):
    url = await asyncio.to_thread(build_product_url, url=url, qs=qs, page=page)
    logging.info(f"[{category.name}][{page}]: Fetching page of url={url}")

    base_filename = f"{category.name}-{page}"
    txt_filename = f"{DEFAULT_DATA_DIR}/{base_filename}.txt"
    json_filename = f"{DEFAULT_OUT_DIR}/{base_filename}.json"

    if not override_exist and (
        os.path.exists(txt_filename) or os.path.exists(json_filename)
    ):
        logging.error(
            f"[{category.name}][{page}]: {base_filename} already exists (no overriding)"
        )
        return page, True, 403

    payload = json.dumps(
        {
            "prefetch-type": "rq",
            "customer-action": "pagination",
        }
    )

    async with AsyncSession(
        cookies=cookies,
        headers=headers,
        proxies=proxies,
    ) as session:
        try:
            resp = await session.request(
                method="POST",
                url=url,
                timeout=60,
                data=payload,
            )
        except curl_cffi.curl.CurlError as e:
            logging.error(
                f"[{category.name}][{page}]: Got error while fetching page: {e}"
            )
            return page, False, 408

        text_data = resp.text

        logging.info(f"[{category.name}][{page}]: Fetched page")

        is_success = True if resp.status_code == 200 else False

        if is_success and text_data.find("data-redirect") != -1:
            logging.error(f"[{category.name}][{page}]: Found redirect message")
            is_success = False

            async with aiofiles.open(f"{base_filename}.redirect.txt", "w") as text_file:
                await text_file.write(text_data)

        if is_success and text_data.find("To discuss automated access") != -1:
            logging.error(f"[{category.name}][{page}]: Amazon detected the scraper")
            is_success = False

            async with aiofiles.open(
                f"{base_filename}.automation_detected.txt", "w"
            ) as text_file:
                await text_file.write(text_data)

        ext = []

        if is_success:
            if text_data.find('"asinOnPageCount\\":0') != -1:
                logging.warning(
                    f"[{category.name}][{page}]: Found the end of the category, stopping"
                )
                should_stop.set()
                return page, is_success, 204

            async with aiofiles.open(txt_filename, "w") as text_file:
                await text_file.write(text_data)
                ext.append("txt")

            logging.info(f"[{category.name}][{page}]: Saved data in .{ext}")

    return page, is_success, resp.status_code


async def process_category(
    category: Category,
    page_start: int = 1,
    page_end: int = 45,
    batch_size: int = 5,
    overwrite: bool = False,
):
    (cookies, _), _ = await get_cookies()

    if page_start < 1:
        page_start = 1

    should_stop = asyncio.Event()

    base, qs = preprocess_url_parts(category.url)

    results = []

    for page in range(page_start, page_end, batch_size):
        logging.info(
            f"[{category.name}][{page_start}:{page_end}:{batch_size}]: Start batch"
        )
        proxy, _ = await get_proxy()
        proxy_str = None if not proxy else proxy.proxies[0]

        proxy, proxy_headers = parse_proxy_str(proxy_str)
        proxies = {} if not proxy else {"http": proxy, "https": proxy}
        headers = {**base_headers, **proxy_headers}

        tasks = []
        for i in range(page, page + batch_size):
            task = asyncio.create_task(
                process_category_page(
                    category,
                    base,
                    qs,
                    i,
                    cookies,
                    headers,
                    proxies,
                    should_stop,
                    overwrite,
                )
            )
            await sleep_randomly(0.16, 0.58)
            tasks.append(task)

        sub_results = await asyncio.gather(*tasks, return_exceptions=True)
        results.extend(sub_results)

        logging.info(
            f"[{category.name}][{page_start}:{page_end}:{batch_size}]: End batch"
        )

        if should_stop.is_set():
            logging.warning(
                f"[{category.name}][{page_start}:{page_end}:{batch_size}]: End-of-category flag is ON, stopped"
            )
            break

    logging.info(f"[{category.name}]: Finished processing")
    return results


# Define your scraping function here
async def execute_pipeline(
    start: int, end: int, step: int, stop: int, depth: int, overwrite: bool
):
    page_start, page_end, batch_size = start, end, step
    max_page_per_category = stop

    tasks = []

    max_tasks = 25

    while page_start <= page_end and (
        max_page_per_category is None or page_start <= max_page_per_category
    ):
        logging.info(f"Start pipeline [{page_start}, {page_end}, {batch_size}]")
        task = asyncio.create_task(
            product_scraping_pipeline(
                page_start=page_start,
                page_end=page_end,
                batch_size=batch_size,
                max_depth=depth,
                max_page_per_category=max_page_per_category,
                max_categories_per_depth=1,
                overwrite=overwrite,
            )
        )
        tasks.append(task)
        page_start = page_end
        page_end = page_end + batch_size

        if len(tasks) >= max_tasks:
            logging.info(f"Reached maximum concurrent pipelines, synchronizing")
            await asyncio.gather(*tasks, return_exceptions=True)
            tasks = []

    await asyncio.gather(*tasks, return_exceptions=True)


async def product_scraping_pipeline(
    *,
    page_start: int = 1,
    page_end: int = 45,
    max_depth: int = 2,
    max_categories_per_depth: int = None,
    batch_size: int = 6,
    max_page_per_category: int = None,
    overwrite: bool = False,
):
    """
    Category scraping strategy:
        - Scrape categories from high-to-low depth
        - After scraping, put all their parents in a set, and put into the processed list
        - Move up
        - Repeat until level < 0
    """
    parents = set()
    results = []

    for depth in range(max_depth, -1, -1):
        logging.info(f"Processing categories of depth = {depth}")
        subparents = set()
        (categories, num_of_categories), _ = await get_categories(
            depth=depth,
            strict=True,
        )
        logging.info(
            f"Current number of categories at depth {depth}: {num_of_categories}"
        )

        if (
            page_end != -1
            and max_page_per_category
            and max_page_per_category <= page_end
        ):
            page_end = min(max_page_per_category, 45)

        if not page_start:
            page_start = 1

        if page_start > page_end:
            page_start = page_end

        if not batch_size:
            batch_size = 15

        if max_categories_per_depth:
            categories = categories[:max_categories_per_depth]

        for category in categories:
            if category.parent in parents:
                logging.error(f"{category.parent} already fully processed, skipping")
                continue

            subresults = await process_category(
                category,
                page_start,
                page_end,
                batch_size,
                overwrite,
            )
            logging.info(f"Result of {category.name}: {subresults}")

            if category.parent is not None:
                await asyncio.to_thread(subparents.add, category.parent)

            await sleep_randomly(1.3, 4.6)
            await asyncio.to_thread(results.extend, (category.name, subresults))

        parents.union(subparents)

    logging.info(f"Final results: {results}")


def main():
    parser = argparse.ArgumentParser(description="Scrape categories from a source.")

    parser.add_argument(
        "--page_start", type=int, default=1, help="Starting page for scraping."
    )
    parser.add_argument(
        "--page_end", type=int, default=45, help="Ending page for scraping."
    )
    parser.add_argument(
        "--max_depth", type=int, default=2, help="Maximum depth for scraping."
    )
    parser.add_argument(
        "--batch_size", type=int, default=15, help="Batch size for scraping."
    )
    parser.add_argument(
        "--max_page_per_category",
        type=int,
        default=None,
        help="Maximum pages per category.",
    )
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    # Execute the scraping
    asyncio.run(
        execute_pipeline(
            start=args.page_start,
            end=args.page_end,
            step=args.batch_size,
            stop=args.max_page_per_category,
            depth=args.max_depth,
            overwrite=args.overwrite,
        )
    )
