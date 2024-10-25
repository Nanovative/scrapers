import random
import logging

from playwright.async_api import (
    Page,
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)
from shared.models.cookie import AmazonCookieRequest
from shared.models.enums import BrowserType

postcode_pool = [
    99501,  # Anchorage
    # 93311,  # Bakersfield
    # 71601,  # Pine Bluff
    # 11001,  # Floral Park
]


def get_new_postcode_pool():
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
    return postcode_pool


get_random_us_postcode = lambda: postcode_pool[
    random.randint(0, len(postcode_pool) - 1)
]


async def get_cookies(body: AmazonCookieRequest):
    async def get_postcode_locator(page: Page):
        await page.wait_for_selector("#nav-global-location-popover-link")
        return page.locator("#nav-global-location-popover-link")

    async def get_zipcode_locator(page: Page):
        await page.wait_for_selector("#GLUXZipUpdateInput")
        return page.locator("#GLUXZipUpdateInput")

    async def get_close_postcode_diag_locator(page: Page):
        await page.wait_for_selector(
            "[class='a-popover-footer'] [class='a-button-input']"
        )
        return page.locator("[class='a-popover-footer'] [class='a-button-input']")

    async def get_invalid_zipcode_locator(page: Page):
        await page.wait_for_selector('span#GLUXZipError[style*="display: inline;"]')
        return page.locator('span#GLUXZipError[style*="display: inline;"]')

    response = {
        "request_id": body.request_id,
        "message": "ok",
        "postcode": get_random_us_postcode(),
        "id": "",
        "cookies": [],
        "html": "",
        "location": "",
    }

    async with async_playwright() as p:
        request_id = body.request_id
        if body.postcode:
            response["postcode"] = body.postcode

        postcode = response["postcode"]

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
                await page.goto(url, timeout=random.randint(8000, 14000))
                logging.info(f"[request_id={body.request_id}]: Went to {url}")

                postcode_locator = await get_postcode_locator(page)
                old_location = await postcode_locator.inner_text(
                    timeout=random.randint(8000, 14000)
                )

                response["location"] = old_location[old_location.index("\n") + 1 :]

                await postcode_locator.click(timeout=random.randint(8000, 14000))
                logging.info(f"[request_id={body.request_id}]: Clicked Postcode button")
                break

            except PlaywrightTimeoutError:
                logging.info(
                    f"[request_id={body.request_id}]: Num of retries left: {retries}"
                )
                retries -= 1
                continue

        if not retries:
            response["message"] = "unable to locate Postcode section"
            return response

        retries = 2

        await page.wait_for_timeout(400)

        zipcode_input_locator = await get_zipcode_locator(page)
        await zipcode_input_locator.fill(str(postcode))
        logging.info(
            f"[request_id={body.request_id}]: Input {postcode} postcode into <input>"
        )

        await page.locator("id=GLUXZipInputSection").get_by_text("Apply").click()
        logging.info(f"[request_id={body.request_id}]: Submitted {postcode} postcode")

        while retries > 0:
            try:
                invalid_zipcode_locator = await get_invalid_zipcode_locator(page)
                if not await invalid_zipcode_locator.count():
                    break

                invalid_zipcode_msg = await invalid_zipcode_locator.inner_text(
                    timeout=random.randint(8000, 14000),
                )
                if invalid_zipcode_msg.strip() != "Please enter a valid US zip code":
                    break

                retries -= 1

            except PlaywrightTimeoutError:
                break

        if not retries:
            response["message"] = f"Invalid postal code: {postcode}"
            return response

        await page.wait_for_timeout(765)

        close_postcode_diag_locator = await get_close_postcode_diag_locator(page)
        await close_postcode_diag_locator.click()
        logging.info(f"[request_id={body.request_id}]: Close popup and wait for reset")

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
