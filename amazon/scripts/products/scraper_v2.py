import __init__
from __init__ import DEFAULT_DATA_DIR, DEFAULT_OUT_DIR

import os
import bs4
import json
import uuid
import asyncio

import logging
import aiofiles
import curl_cffi
import urllib.parse as urlparser

from enum import Enum
from typing import Optional
from config import base_headers
from shared.models.proxy import Proxy
from shared.utils import sleep_randomly
from shared.models.category import Category
from curl_cffi.requests import AsyncSession


class SortTendency(Enum):
    FEATURED = "featured-rank"
    NEWEST_PRODUCTS = "date-desc-rank"
    BESTSELLERS = "exact-aware-popularity-rank"
    AVG_CUSTOMER_REVIEWS = "review-rank"
    HIGHEST_PRICE = "price-desc-rank"
    LOWEST_PRICE = "price-asc-rank"


API_URL = os.getenv("API_URL", "http://localhost:8000")
FETCH_PAYLOAD = json.dumps(
    {
        "prefetch-type": "rq",
        "customer-action": "pagination",
    }
)


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


def build_product_url(
    url: str,
    qs: dict[str, str],
    page: int,
    sort_tendency: SortTendency = SortTendency.NEWEST_PRODUCTS,
):
    final_qs = {**qs, "page": page, "ref": f"sr_pg_{page}", "s": sort_tendency.value}
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


async def fetch_txt(
    category: Category,
    url: str,
    page: int,
    async_session: AsyncSession,
):
    logging.info(f"[{category.name}][{page}]: Fetching page of url={url}")
    try:
        resp = await async_session.request(
            method="POST",
            url=url,
            timeout=60,
            data=FETCH_PAYLOAD,
        )
    except curl_cffi.curl.CurlError as e:
        logging.error(f"[{category.name}][{page}]: Got error while fetching page: {e}")
        return None, False, 408

    text_data = resp.text

    logging.info(f"[{category.name}][{page}]: Fetched page")

    is_success = True if resp.status_code == 200 else False

    if is_success and text_data.find("data-redirect") != -1:
        logging.error(f"[{category.name}][{page}]: Found redirect message")
        is_success = False

    if is_success and text_data.find("To discuss automated access") != -1:
        logging.error(f"[{category.name}][{page}]: Amazon detected the scraper")
        is_success = False

    logging.info(f"[{category.name}][{page}]: Successfully fetched HTML TXT")

    if is_success:
        return text_data, True, 200

    return None, False, 400


def preprocess_txt(content: str):
    json_data = content.replace("&&&", ",", content.count("&&&") - 1).replace("&&&", "")
    new_json_data = {
        "metadata": {},
        "data": [],
    }
    data_exclude_keys = {"index", "data"}

    for i, record in enumerate(json.loads(f"[{json_data}]")):
        if any(
            record[1].find(kw) != -1
            for kw in {"data-search-metadata", "data-main-slot:search-result"}
        ):
            k = "metadata" if record[1] == "data-search-metadata" else "data"
            if k == "metadata":
                new_json_data["metadata"] = record[2]["metadata"]
            else:
                new_json_record = {
                    k: v for k, v in record[2].items() if k not in data_exclude_keys
                }
                new_json_data["data"].append(new_json_record)

    new_json_data["metadata"]["actualTotalResultCount"] = len(new_json_data["data"])

    return new_json_data


def postprocess_json(json_data: dict):
    asins = []
    for idx, record in enumerate(json_data["data"]):
        parsed = bs4.BeautifulSoup(record["html"], features="html.parser")
        if "seeing this ad based on the product" in parsed.text:
            json_data["data"][idx] = None

        else:
            uuid = None
            asin = record["asin"]
            title = None
            url = None
            avg_rating = None
            total_review = None
            sale_count = None
            item_price = None

            product_block = parsed.find("div", attrs={"data-uuid": True})
            if product_block:
                uuid = product_block.attrs.get("data-uuid")

            h2_tags = parsed.find("div", attrs={"data-cy": "title-recipe"}).find_all(
                "h2"
            )
            if len(h2_tags) > 1:
                h2_tags = h2_tags[1:]

            title_block = h2_tags[0]
            if title_block:
                title = title_block.get_text().strip()
                title_block = title_block.find("a")
                if title_block:
                    url = f"https://amazon.com/{title_block.attrs.get('href')}"

            review_block = parsed.find("div", attrs={"data-cy": "reviews-block"})

            if review_block:
                review_and_rating_block = review_block.select_one(
                    "div.a-row.a-size-small",
                )

                sale_block = review_block.select_one(
                    "div.a-row.a-size-base",
                )

                if review_and_rating_block:
                    if avg_rating_block := review_and_rating_block.find(
                        "i", attrs={"data-cy": "reviews-ratings-slot"}
                    ):
                        avg_rating = avg_rating_block.get_text()
                        avg_rating = float(avg_rating.split("out of")[0].strip())

                    if total_review_block := review_and_rating_block.find(
                        "a", attrs={"class": ["a-size-base", "s-underline-text"]}
                    ):
                        total_review = int(
                            total_review_block.get_text().replace(",", "").strip()
                        )

                if sale_block:
                    if sale_count := sale_block.find(
                        "span", attrs={"class": ["a-size-base", "a-color-secondary"]}
                    ):
                        sale_count = sale_count.get_text()
                        sale_count = sale_count.split(" ")[0].strip()

            price_block = parsed.find("div", attrs={"data-cy": "price-recipe"})
            if price_block:
                first_price_block = price_block.find("div")
                if first_price_block:
                    first_price_block = first_price_block.find(
                        "span", attrs={"class": "a-price"}
                    )
                if first_price_block:
                    item_price = first_price_block.get_text()
                    item_price = float(
                        item_price[1 : item_price.find("$", 1)].replace(",", "")
                    )

            del json_data["data"][idx]["html"]
            json_data["data"][idx] = {
                **json_data["data"][idx],
                "uuid": uuid,
                "title": title,
                "url": url,
                "avg_rating": avg_rating,
                "total_review": total_review,
                "sale_count": sale_count,
                "price": item_price,
            }
            asins.append(asin)

    json_data["data"] = [node for node in json_data["data"] if node is not None]
    json_data["metadata"]["asins"] = asins
    return json_data


async def process_category(
    category: Category, overwrite: bool, product_cap: int, processed_asins: set[str]
):
    should_stop = asyncio.Event()
    overlapped = asyncio.Event()

    overlap_threshold = 95.0
    product_count = 0
    overlap_limit = 10
    rotation_batch = 3

    base_url, qs = preprocess_url_parts(category.url)
    page = 0

    cookies: dict[str, str] = {}
    proxies: dict[str, str] = {}
    headers: dict[str, str] = {}

    (cookies, _), _ = await get_cookies()
    proxy, _ = await get_proxy()
    proxy_str = None if not proxy else proxy.proxies[0]

    proxy, proxy_headers = parse_proxy_str(proxy_str)
    proxies = {} if not proxy else {"http": proxy, "https": proxy}
    headers = {**base_headers, **proxy_headers}

    async_session = AsyncSession(
        cookies=cookies,
        headers=headers,
        proxies=proxies,
    )

    results = []

    while True:
        page += 1
        page_id = f"{page}-{uuid.uuid4()}"
        if should_stop.is_set():
            logging.info(
                f"[{category.name}][{page}]: Stopped processing (end of category)"
            )
            break

        if overlapped.is_set():
            logging.info(
                f"[{category.name}][{page}]: Stopped processing (found overlapped products)"
            )
            break

        if product_count >= product_cap:
            logging.info(
                f"[{category.name}][{page}]: Exceeded product cap ({product_count}/{product_cap})"
            )
            break

        url = await asyncio.to_thread(build_product_url, url=base_url, qs=qs, page=page)
        base_filename = f"{category.name}-{page_id}"
        json_filename = f"{DEFAULT_DATA_DIR}/{base_filename}.json"

        json_data = None

        content, is_success, status_code = await fetch_txt(
            category=category,
            url=url,
            page=page,
            async_session=async_session,
        )
        if content is not None:
            json_data = await asyncio.to_thread(preprocess_txt, content)
            logging.info(
                f"[{category.name}][{page}]: Preprocessed raw TXT to dict data"
            )
            json_data = await asyncio.to_thread(postprocess_json, json_data)
            logging.info(f"[{category.name}][{page}]: Postprocessed dict data")
            json_str = await asyncio.to_thread(
                json.dumps, json_data, separators=(",", ":")
            )
            logging.info(f"[{category.name}][{page}]: Dumped dict data to JSON string")

        if json_data is not None:
            page_metadata: dict = json_data["metadata"]
            in_page_count: int = page_metadata["asinOnPageCount"]
            approx_total_count: int = page_metadata["totalResultCount"]
            actual_total_count: int = page_metadata["actualTotalResultCount"]
            page_asins: set[str] = set(page_metadata["asins"])

            overlap_ratio = len(page_asins.intersection(processed_asins)) / len(
                page_asins
            )
            overlap_perc = overlap_ratio * 100

            if is_overlapped := overlap_perc > overlap_threshold:
                overlap_limit -= 1
                logging.warning(
                    f"[{category.name}][{page}]: "
                    + f"Found overlapping products "
                    + f"({overlap_perc}% > {overlap_threshold}%) ({overlap_limit} times left)"
                )
                if overlap_limit < 0:
                    overlapped.set()

            product_count = product_count + actual_total_count
            logging.info(
                f"[{category.name}][{page}]: Product count after update = {product_count}"
            )

            if approx_total_count == in_page_count or (
                approx_total_count != in_page_count and in_page_count < 5
            ):
                logging.warning(
                    f"[{category.name}][{page}]: Found the end of the category, stopping"
                )
                should_stop.set()
                status_code = 204

            async with aiofiles.open(json_filename, "w") as json_file:
                logging.info(f"[{category.name}][{page}]: Dumping JSON string to file")
                await json_file.write(json_str)
                logging.info(f"[{category.name}][{page}]: Dumped JSON string to file")

        # Refresh cookies & proxies
        if (page - 1) % rotation_batch == 0:
            await async_session.close()
            async_session = AsyncSession(
                cookies=cookies,
                headers=headers,
                proxies=proxies,
            )

            (cookies, _), _ = await get_cookies()
            proxy, _ = await get_proxy()
            proxy_str = None if not proxy else proxy.proxies[0]

            proxy, proxy_headers = parse_proxy_str(proxy_str)
            proxies = {} if not proxy else {"http": proxy, "https": proxy}
            headers = {**base_headers, **proxy_headers}

    return results


async def execute_pipeline(
    max_products_per_category: int, max_categories_per_depth: int = -1
):
    processed_parents = set()
    tasks = []

    max_depth = 5

    for depth in range(max_depth, 0, -1):
        (categories, num_of_categories), status_code = await get_categories(depth)
        asins_map = {category.name: set() for category in categories}
        assert status_code == 200, "Can't fetch categories"
        product_json_files = await asyncio.to_thread(os.listdir, DEFAULT_DATA_DIR)
        for product_json_file in product_json_files:
            product_file_category = product_json_file.split("-")[0]
            if not asins_map.get(product_file_category, None):
                asins_map[product_file_category] = set()

            async with aiofiles.open(f"{DEFAULT_DATA_DIR}/{product_json_file}") as file:
                json_str = await file.read()
                json_data = await asyncio.to_thread(json.loads, json_str)
                asins = json_data["metadata"]["asins"]
                asins_map[product_file_category] = asins_map[
                    product_file_category
                ].union(set(asins))

        if max_categories_per_depth > -1:
            categories = categories[:max_categories_per_depth]
            num_of_categories = max_categories_per_depth

        processed_subparents = set()

        for category in categories[:num_of_categories]:
            # if category.name in processed_parents:
            #     logging.warning(
            #         f"[{category.name}]: Descendants are processed, skipping this one"
            #     )
            #     continue

            task = asyncio.create_task(
                process_category(
                    category, True, max_products_per_category, asins_map.get(category.name, set())
                )
            )
            logging.info(f"Created task to process category {category.name}")
            tasks.append(task)

            if category.parent is not None:
                await asyncio.to_thread(processed_subparents.add, category.parent)

        await asyncio.gather(*tasks, return_exceptions=True)
        processed_parents.union(processed_subparents)


if __name__ == "__main__":
    from shared.config.logger import setup_logger

    setup_logger()
    asyncio.run(execute_pipeline(50000, -1))
