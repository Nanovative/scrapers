import asyncio

from services.amazon_cookie_pool import start_add_task, start_cleanup_task
from api import event_queue, event_loop_lock, get_cookie_set_pool


async def _cookie_pool_fill(event_loop: asyncio.AbstractEventLoop):
    print("Start cookie pool fill task")
    cookie_set_pool = await get_cookie_set_pool()
    try:
        msg = {
            "fn": start_add_task,
            "args": (cookie_set_pool, event_loop_lock, False),
        }
        event_queue.put_nowait(msg)
    except asyncio.QueueFull:
        print("event queue is full, waiting...")


async def _cookie_pool_process(event_loop: asyncio.AbstractEventLoop):
    print("Start cookie pool process task")
    try:
        msg = event_queue.get_nowait()
        fn, args = msg["fn"], msg["args"]
        event_loop.create_task(fn(*args))

    except asyncio.QueueEmpty:
        print("event queue is empty, waiting...")


async def _cookie_pool_cleanup(event_loop: asyncio.AbstractEventLoop):
    print("Start cookie pool cleanup task")
    cookie_set_pool = await get_cookie_set_pool()
    event_loop.create_task(start_cleanup_task(cookie_set_pool, event_loop_lock))


async def schedule_cookie_pool_fill(
    event_loop: asyncio.AbstractEventLoop, sleep_interval=12.2
):
    print("Schedule cookie pool fill task")
    while True:
        await _cookie_pool_fill(event_loop)
        await asyncio.sleep(sleep_interval)


async def schedule_cookie_pool_process(
    event_loop: asyncio.AbstractEventLoop, sleep_interval=18.4
):
    print("Schedule cookie pool process task")
    while True:
        print("queue size:", event_queue.qsize())
        await _cookie_pool_process(event_loop)
        await asyncio.sleep(sleep_interval)


async def schedule_cookie_pool_cleanup(
    event_loop: asyncio.AbstractEventLoop, sleep_interval=74.5
):
    print("Schedule cookie pool cleanup task")
    while True:
        print("queue size:", event_queue.qsize())
        await _cookie_pool_cleanup(event_loop)
        await asyncio.sleep(sleep_interval)
