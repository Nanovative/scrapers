from services.amazon_cookie_pool import get_amazon_cookies, AmazonCookieRequest
from api import get_cookie_set_pool, event_loop_lock
from fastapi import APIRouter

router = APIRouter()


async def _get_new(body: AmazonCookieRequest):
    print("execute new playwright request")
    return await get_amazon_cookies(body)


async def _fetch_pool(body: AmazonCookieRequest):
    print("fetching from pool")
    cookie_set_pool = await get_cookie_set_pool()
    cookie_set = await cookie_set_pool.get(body.browser_type, event_loop_lock)
    if cookie_set:
        return {
            "message": "ok",
            "cookies": cookie_set.cookies,
            "html": "",
            "location": cookie_set.location,
        }

    return None


@router.post("/fill")
async def fill_amazon_cookies(body: AmazonCookieRequest):

    response = await _get_new(body)
    cookie_set_pool = await get_cookie_set_pool()
    pool_add_ok = await cookie_set_pool.add(
        body.browser_type,
        response["postcode"],
        response["location"],
        response["cookies"],
        event_loop_lock,
    )

    return {**response, "pool_add_ok": pool_add_ok}


@router.post("/fetch")
async def fetch_amazon_cookies(body: AmazonCookieRequest):

    if not body.do_fetch_pool:
        response = await _get_new(body)
    else:
        response = await _fetch_pool(body)
        if response is None:
            response = {
                "message": "pool empty",
                "cookies": [],
                "html": "",
                "location": "",
            }

    return response
