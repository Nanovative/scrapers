import os
import json
import asyncio
import logging
import aiofiles

from cookies import cookies as static_cookies
from typing import Any
from urllib.parse import urlparse, parse_qs, urlencode
from utils import AsyncSafeDict

from playwright.async_api import (
    Page,
    async_playwright,
    BrowserType,
    Cookie,
    Playwright,
    Browser,
    BrowserContext,
)

ROOT_URL = "https://amazon.com"
OUT_DIR = "./json"


async def init_browser(
    is_headless: bool, browser_type_str: str, cookies: list[Cookie] = None
):
    playwright = await async_playwright().start()
    browser_type = None

    if browser_type_str == "chromium":
        browser_type = playwright.chromium
    elif browser_type_str == "firefox":
        browser_type = playwright.firefox

    assert isinstance(
        browser_type, BrowserType
    ), "Unsupported browser_type_str [firefox, chromium]"

    browser = await browser_type.launch(headless=is_headless)
    context = await browser.new_context()
    if cookies and isinstance(cookies, list):
        await context.add_cookies(cookies)

    return playwright, browser, context


def process_category_url(url: str) -> str:
    parsed_url = urlparse(url)
    qs = parse_qs(parsed_url.query)
    qs.pop("k", None)  # Remove unwanted query params
    qs = {k: v[0] for k, v in qs.items()}  # Flatten list values
    qs["fs"] = "true"  # Add fixed query param
    full_path = f"{ROOT_URL}{parsed_url.path}?{urlencode(qs)}"
    return full_path


async def extract_sub_categories(page: Page, parent_category_name: str):
    logging.info(
        f"Extracting subcategories from {parent_category_name}",
    )
    items = await page.query_selector_all(
        ".a-spacing-micro.s-navigation-indent-2 a.a-link-normal.s-navigation-item"
    )
    data = {}
    if not len(items):
        logging.info(
            f"{parent_category_name}: no expected subcategories",
        )

        # This phenomenon could be found in Baby/Baby Clothing & Shoes/Baby Girls' Clothing & Shoes
        # I covered this but it seems not to be what we want, so I commented it out

        # if not await page.query_selector(".a-spacing-micro .a-text-bold"):
        #     items = await page.query_selector_all(
        #         "#departments [class='a-spacing-micro'] a.a-link-normal.s-navigation-item"
        #     )
        #     logging.info(
        #         f"{parent_category_name}: trying to look for normal links, current normal links: {len(items)}",
        #     )
        #     for item in items:
        #         logging.info(await item.inner_text())

    for item in items:
        name_element = await item.query_selector("span.a-size-base.a-color-base")
        category_name = await name_element.inner_text() if name_element else "No name"
        logging.info(f"Extracting href and name from {category_name}")
        href = await item.get_attribute("href")

        if href:
            new_url = await asyncio.to_thread(process_category_url, href)
            data[category_name] = {
                "url": new_url,
                "inner": {},
            }

    return data


async def explore_inner(
    page: Page,
    inner: dict[str, Any],
    explored: AsyncSafeDict,
    search_width=None,
):
    # Base case: inner is {}

    if not len(inner):
        logging.info(f"Encounter leaf category -> exit")
        return {}

    new_inner = inner

    for category_name in new_inner:
        if await explored.get(category_name):
            logging.info(
                f"{category_name} found in explored categories -> ignore",
            )
            continue

        await explored.set(category_name, True)

        url = new_inner[category_name]["url"]
        logging.info(f"(category_name, url) = ({category_name}, {url})")

        if search_width is not None and not search_width:
            break
        # Explore subcategories
        await page.goto(url, timeout=0, wait_until="domcontentloaded")

        logging.info(f"Exploring inner of {category_name}")
        sub_categories = await extract_sub_categories(page, category_name)
        new_inner_inner = await explore_inner(
            page, sub_categories, explored, search_width
        )
        new_inner[category_name]["inner"] = new_inner_inner

        if search_width is not None:
            search_width -= 1

    return new_inner


async def get_sub_categories_from_root_categories(
    p: Playwright, browser: Browser, context: BrowserContext, page: Page, keyword: str
):
    data = {}
    try:
        await page.goto(ROOT_URL, timeout=0, wait_until="domcontentloaded")
        await page.wait_for_load_state("load", timeout=60000)

        dropdown_locator = page.locator("#searchDropdownBox")
        option_count = await dropdown_locator.locator("option").count()

        for i in range(option_count):
            try:
                option = dropdown_locator.locator("option").nth(i)
                option_text = await option.inner_text()
                option_value = await option.get_attribute("value")

                # Only process the "keyword" option
                if option_text != keyword:
                    continue

                await dropdown_locator.select_option(value=option_value)
                search_box_locator = page.locator("#twotabsearchtextbox")
                await search_box_locator.fill(option_text)
                await search_box_locator.press("Enter")

                await page.wait_for_load_state("load", timeout=60000)

                logging.info(f"Searched for {option_text}")

                # Extract URLs and names
                sub_categories = await extract_sub_categories(page, keyword)

                # We were meant to dump this file to a folder, but my approach would embed them into a whole json

                # await asyncio.to_thread(os.makedirs, name=option_text, exist_ok=True)
                # async with aiofiles.open(
                #     os.path.join(option_text, "url.json"), "w", encoding="utf-8"
                # ) as file:
                #     json_data = await asyncio.to_thread(
                #         json.dumps, obj=sub_categories, ensure_ascii=False, indent=2
                #     )
                #     await file.write(json_data)

                data = {
                    "path": keyword,
                    "url": page.url,
                    "inner": sub_categories,
                }

                # Go back to the previous page
                await page.go_back()
                await page.wait_for_load_state("load", timeout=60000)

            except Exception as e:
                logging.error(f"Error occurred for option {i}: {e}")

    except Exception as e:
        logging.error(f"Error processing {keyword}: {e}")

    finally:
        return data


async def process_keyword(
    p: Playwright,
    browser: Browser,
    context: BrowserContext,
    keyword: str,
):
    data = {}
    explored = AsyncSafeDict()
    try:
        page = await context.new_page()
        data = await get_sub_categories_from_root_categories(
            p,
            browser,
            context,
            page,
            keyword,
        )

        if data:
            data["inner"] = await explore_inner(
                page,
                data["inner"],
                explored,
            )

    except Exception as e:
        logging.error(f"Encounter error while walking category tree of {keyword}: {e}")

    finally:
        logging.info(f"Finished walking category tree of {keyword}")
        await page.close()
        filename = f"{OUT_DIR}/{keyword}.json"
        async with aiofiles.open(filename, "w", encoding="utf-8") as file:
            json_data = await asyncio.to_thread(
                json.dumps, obj=data, ensure_ascii=False, indent=2
            )
            await file.write(json_data)
            logging.info(f"Done dumping category tree of {keyword} to {filename}")

    return data, keyword


async def scrape_category_tree(
    is_headless: bool, browser_type_str: str, keywords: list[str]
):
    os.makedirs(OUT_DIR, exist_ok=True)

    data = {}
    p, browser, context = await init_browser(
        is_headless, browser_type_str, static_cookies
    )
    # Create a separate page
    event_loop = asyncio.get_running_loop()
    try:
        tasks = [
            event_loop.create_task(
                process_keyword(p, browser, context, keyword), name=f"process {keyword}"
            )
            for keyword in keywords
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(f"Task {i + 1} failed with exception: {result}")
            else:
                res, kw = result
                logging.info(f"Task {i + 1} processing {kw} succeeded")
                data[kw] = res

    finally:
        logging.info("Gracefully cleaning up Playwright remnants...")
        await browser.close()
        await p.stop()
        filename = OUT_DIR + " | ".join(keywords) + ".aggregated.json"
        async with aiofiles.open(filename, "w", encoding="utf-8") as file:
            json_data = await asyncio.to_thread(
                json.dumps, obj=data, ensure_ascii=False, indent=2
            )
            await file.write(json_data)
            logging.info(f"Done dumping all keywords to {filename}")
