from service import get_amazon_cookies, AmazonCookieRequest, pool
from model import event_loop_lock
from fastapi import FastAPI

app = FastAPI()


async def _get_new(body: AmazonCookieRequest):
    print("execute new playwright request")
    return await get_amazon_cookies(body)


async def _fetch_pool(body: AmazonCookieRequest):
    print("fetching from pool")
    cookie_set = await pool.get(body.browser_type, event_loop_lock)
    if cookie_set:
        return {
            "message": "ok",
            "cookies": cookie_set.cookies,
            "html": "",
            "location": cookie_set.location,
        }

    return None


@app.post("/cookies/amazon/fill")
async def _fill_amazon_cookies(body: AmazonCookieRequest):

    response = await _get_new(body)
    pool_add_ok = await pool.add(
        body.browser_type,
        response["postcode"],
        response["location"],
        response["cookies"],
        event_loop_lock,
    )

    return {**response, "pool_add_ok": pool_add_ok}


@app.post("/cookies/amazon/fetch")
async def _fetch_amazon_cookies(body: AmazonCookieRequest):

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

@app.get("/cookies/amazon/pool_size")
async def _get_amazon_cookie_pool_size(body: AmazonCookieRequest):
    pool_sizes = {
        browser_type: {
            "current": await pool.pool_size(browser_type),
            "max": await pool.max_pool_size(browser_type),
        }
        for browser_type in pool._pool_weight
    }
    response = {
        "message": "ok",
        "size": pool_sizes,
    }
    return response
