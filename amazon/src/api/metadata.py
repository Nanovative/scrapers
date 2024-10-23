import uuid

from api import get_cookie_set_pool, event_queue
from fastapi import APIRouter

router = APIRouter()


@router.get("/cookie/pool")
async def get_amazon_cookie_pool_size():
    cookie_set_pool = await get_cookie_set_pool()
    pool_sizes = {
        browser_type: {
            "current": await cookie_set_pool.pool_size(browser_type),
            "max": await cookie_set_pool.max_pool_size(browser_type),
        }
        for browser_type in cookie_set_pool._browser_types
    }
    response = {
        "request_id": uuid.uuid4(),
        "message": "ok",
        "size": pool_sizes,
    }
    return response


@router.get("/cookie/task")
async def get_amazon_cookie_event_queue_size():

    queue_size = event_queue.qsize()
    response = {
        "request_id": uuid.uuid4(),
        "message": "ok",
        "size": queue_size,
    }
    return response
