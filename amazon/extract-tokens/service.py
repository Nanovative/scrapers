import asyncio
import random
from playwright.async_api import (
    async_playwright,
    Page,
    TimeoutError as PlaywrightTimeoutError,
)
from model import BrowserType, AmazonCookieRequest, pool


postcode_pool = [
    random.randint(99501, 99950),  # Alaska
    random.randint(35004, 36925),  # Alabama
    random.randint(71601, 72959),  # Arkansas
    random.randint(96799, 96799),  # Soamoa
    random.randint(85001, 86556),  # Arizona
    random.randint(90001, 96162),  # California
    random.randint(80001, 81658),  # Colorado
    random.randint(43001, 45999),  # Ohio
]

postcode_pool = [
    99501,  # Anchorage
    93311,  # Bakersfield
    71601,  # Pine Bluff
    11001,  # Floral Park
]

get_random_us_postcode = lambda: postcode_pool[
    random.randint(0, len(postcode_pool) - 1)
]


async def get_amazon_cookies(body: AmazonCookieRequest):
    async def get_postcode_locator(page: Page):
        return page.locator("id=nav-global-location-popover-link")

    async def get_zipcode_locator(page: Page):
        return page.locator("id=GLUXZipUpdateInput")

    async def get_close_postcode_diag_locator(page: Page):
        return page.locator("[class=a-popover-footer]").locator(
            "[class=a-button-input]"
        )

    async def get_invalid_zipcode_locator(page: Page):
        return page.locator("id=GLUXZipError").locator('[style*="display:inline"]')

    response = {"message": "ok", "cookies": {}, "html": "", "location": ""}

    async with async_playwright() as p:
        postcode = body.postcode
        if not postcode:
            postcode = get_random_us_postcode()

        url = "https://www.amazon.com/ref=nav_bb_logo"

        if body.browser_type == BrowserType.firefox:
            browser_type = p.firefox
        elif body.browser_type == BrowserType.chromium:
            browser_type = p.chromium

        browser = await browser_type.launch(headless=body.is_headless)

        context = await browser.new_context()
        page = await context.new_page()

        page.set_default_timeout(body.max_timeout)

        retries = 3

        while retries > 0:
            try:
                await page.goto(url, timeout=12000)
                print(f"went to {url}")

                postcode_locator = await get_postcode_locator(page)
                old_location = await postcode_locator.inner_text(timeout=12000)

                response["location"] = old_location[old_location.index("\n") + 1 :]

                await postcode_locator.click(timeout=12000)
                print(f"clicked Postcode button")
                break

            except PlaywrightTimeoutError:
                print(
                    "num of retries left:",
                    retries,
                )
                retries -= 1
                continue

        if not retries:
            response["message"] = "unable to locate Postcode section"
            return response

        retries = 2

        await page.wait_for_timeout(400)

        while retries > 0:

            zipcode_input_locator = await get_zipcode_locator(page)
            await zipcode_input_locator.fill(str(postcode))
            print(f"input {postcode} postcode into <input>")

            await page.locator("id=GLUXZipInputSection").get_by_text("Apply").click()
            print(f"submitted {postcode} postcode")

            try:
                invalid_zipcode_locator = await get_invalid_zipcode_locator(page)
                invalid_zipcode_msg = await invalid_zipcode_locator.inner_text(
                    timeout=3000
                )
                if invalid_zipcode_msg.strip() != "Please enter a valid US zip code":
                    break

                retries -= 1

            except PlaywrightTimeoutError:
                break

        if not retries:
            response["message"] = f"invalid postal code: {postcode}"
            return response

        await page.wait_for_timeout(765)

        close_postcode_diag_locator = await get_close_postcode_diag_locator(page)
        await close_postcode_diag_locator.click()
        print(f"close the popup and wait for reset")

        await page.goto(url)

        await page.wait_for_timeout(200)

        postcode_locator = await get_postcode_locator(page)
        new_location = await postcode_locator.inner_text()

        response["location"] = new_location[new_location.index("\n") + 1 :]

        if body.include_html:
            html = await page.content()
            response["html"] = html

        if old_location == new_location:
            response["message"] = "postcode not changed"

        cookies = await context.cookies()
        response["cookies"] = cookies

        await context.close()
        await browser.close()

    return response


async def start_cleanup_task(lock: asyncio.Lock = None):
    for browser_type in pool._pool_weight.keys():
        await pool.clean(browser_type, lock)


async def start_add_task(lock: asyncio.Lock = None, is_independent_loop=True):
    async def helper():
        for browser_type in pool._pool_weight.keys():
            postcode = get_random_us_postcode()
            body = AmazonCookieRequest(
                postcode=postcode,
                include_html=False,
                is_headless=True,
                max_timeout=15000,
                browser_type=browser_type,
            )
            resp = await get_amazon_cookies(body)
            cookies = resp["cookies"]
            location = resp["location"]
            print("task message:", resp["message"], resp["location"])
            await pool.add(browser_type, postcode, location, cookies, lock)

    if is_independent_loop:
        while True:
            await helper()
            await asyncio.sleep(1)

    else:
        await helper()
