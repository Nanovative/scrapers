import uuid

from fastapi import APIRouter, Body
from shared.models.proxy import ProxyRequest
from api.utils import get_proxy_pool, event_loop_lock

router = APIRouter()


@router.post("/replace")
async def replace_proxy(body: ProxyRequest):
    request_id = uuid.uuid4()
    body.request_id = request_id

    proxy_pool = await get_proxy_pool()

    is_ok = await proxy_pool.replace(
        body.proxies, body.tag, body.provider, None, event_loop_lock
    )

    response = {
        "request_id": body.request_id,
        "message": "ok" if is_ok else "failed",
    }

    return response


@router.post("/rotate")
async def rotate_proxy(body: ProxyRequest):
    request_id = uuid.uuid4()
    body.request_id = request_id

    proxy_pool = await get_proxy_pool()
    proxy = await proxy_pool.rotate(body.tag, body.provider, None, event_loop_lock)

    response = {
        "request_id": body.request_id,
        "message": "ok",
        "proxies": [],
    }

    if not proxy:
        response["message"] = "pool empty"

    else:
        response["proxies"] = [proxy]

    return response


@router.post("/format")
async def rotate_proxy(body: str = Body(..., media_type="text/plain")):
    request_id = uuid.uuid4()

    proxies = [line.strip("\r") for line in body.split("\n")]

    return {
        "request_id": request_id,
        "proxies": proxies,
    }
