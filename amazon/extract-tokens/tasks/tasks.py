import asyncio
import uuid

from services.amazon_cookie_pool import start_add_task, start_cleanup_task
from api import event_queue, event_loop_lock, get_cookie_set_pool


async def _cookie_pool_fill(
    event_loop: asyncio.AbstractEventLoop, coroutine_id: uuid.UUID
):
    print(f"[coroutine_id={coroutine_id}]: Start cookie pool fill task")
    cookie_set_pool = await get_cookie_set_pool()
    try:
        msg = {
            "fn": start_add_task,
            "args": {
                "pool": cookie_set_pool,
                "coroutine_id": coroutine_id,
                "lock": event_loop_lock,
                "is_independent_loop": False,
            },
        }
        event_queue.put_nowait(msg)
    except asyncio.QueueFull:
        print(f"[coroutine_id={coroutine_id}]: Event queue is full, waiting...")


async def _cookie_pool_process(
    event_loop: asyncio.AbstractEventLoop, coroutine_id: uuid.UUID
):
    print(f"[coroutine_id={coroutine_id}]: Start cookie pool process task")
    print(f"[coroutine_id={coroutine_id}]: Queue size:", event_queue.qsize())
    try:
        msg = event_queue.get_nowait()
        fn, args = msg["fn"], msg["args"]
        event_loop.create_task(fn(**args))

    except asyncio.QueueEmpty:
        print(f"[coroutine_id={coroutine_id}]: Event queue is empty, waiting...")


async def _cookie_pool_cleanup(
    event_loop: asyncio.AbstractEventLoop, coroutine_id: uuid.UUID
):
    print(f"[coroutine_id={coroutine_id}]: Start cookie pool cleanup task")
    print(f"[coroutine_id={coroutine_id}]: Queue size:", event_queue.qsize())
    cookie_set_pool = await get_cookie_set_pool()
    fn, args = start_cleanup_task, {
        "pool": cookie_set_pool,
        "coroutine_id": coroutine_id,
        "lock": event_loop_lock,
    }
    event_loop.create_task(fn(**args))


async def schedule_cookie_pool_fill(
    event_loop: asyncio.AbstractEventLoop, sleep_interval=12.2
):
    print("[MAIN]: Schedule cookie pool fill task")
    while True:
        coroutine_id = uuid.uuid4()
        await _cookie_pool_fill(event_loop, coroutine_id)
        await asyncio.sleep(sleep_interval)


async def schedule_cookie_pool_process(
    event_loop: asyncio.AbstractEventLoop, sleep_interval=18.4
):
    print("[MAIN]: Schedule cookie pool process task")
    while True:
        coroutine_id = uuid.uuid4()
        await _cookie_pool_process(event_loop, coroutine_id)
        await asyncio.sleep(sleep_interval)


async def schedule_cookie_pool_cleanup(
    event_loop: asyncio.AbstractEventLoop, sleep_interval=74.5
):
    print("[MAIN]: Schedule cookie pool cleanup task")
    while True:
        coroutine_id = uuid.uuid4()
        await _cookie_pool_cleanup(event_loop, coroutine_id)
        await asyncio.sleep(sleep_interval)
