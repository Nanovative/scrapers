import uuid

from fastapi import APIRouter
from services.amazon_cookie_pool import get_amazon_cookies, AmazonCookieRequest
from api import get_cookie_set_pool, event_loop_lock
from utils import request_print

router = APIRouter()


async def _get_new(body: AmazonCookieRequest):
    request_print(body.request_id, "Execute new playwright request")
    return await get_amazon_cookies(body)


async def _fetch_cookie_pool(body: AmazonCookieRequest):
    request_print(body.request_id, "Fetching from pool")
    cookie_set_pool = await get_cookie_set_pool()

    old_pool_size = await cookie_set_pool.pool_size(body.browser_type)
    cookie_set = await cookie_set_pool.get(body.browser_type, None, event_loop_lock)
    new_pool_size = await cookie_set_pool.pool_size(body.browser_type)
    request_print(
        body.request_id,
        f"Pool size before/after adding: [{old_pool_size}/{new_pool_size}]",
    )
    if cookie_set:
        return {
            "request_id": body.request_id,
            "message": "ok",
            "id": cookie_set.id,
            "postcode": cookie_set.postcode,
            "cookies": cookie_set.cookies,
            "html": "",
            "location": cookie_set.location,
        }

    return None


@router.post("/fill")
async def fill_amazon_cookies(body: AmazonCookieRequest):
    request_id = uuid.uuid4()
    body.request_id = request_id

    response = await _get_new(body)
    cookie_set_pool = await get_cookie_set_pool()

    if response["message"] != "ok":
        return {"request_id": body.request_id, **response, "pool_add_ok": False}

    old_pool_size = await cookie_set_pool.pool_size(body.browser_type)

    pool_add_ok = await cookie_set_pool.add(
        body.browser_type,
        response["postcode"],
        response["location"],
        response["cookies"],
        event_loop_lock,
    )
    new_pool_size = await cookie_set_pool.pool_size(body.browser_type)
    request_print(
        body.request_id,
        f"Pool size before/after adding: [{old_pool_size}/{new_pool_size}]",
    )

    return {"request_id": body.request_id, **response, "pool_add_ok": pool_add_ok}


@router.post("/fetch")
async def fetch_amazon_cookies(body: AmazonCookieRequest):
    request_id = uuid.uuid4()
    body.request_id = request_id

    if not body.do_fetch_pool:
        response = await _get_new(body)
    else:
        response = await _fetch_cookie_pool(body)
        if response is None:
            response = {
                "request_id": body.request_id,
                "message": "pool empty",
                "id": "",
                "postcode": -1,
                "cookies": [],
                "html": "",
                "location": "",
            }

    return response
