import asyncio
import uuid
import logging

from shared.services.cookie_set_pool import (
    start_add_task,
    start_cleanup_task,
    AmazonCookieSetPool,
)


async def _cookie_pool_fill(
    cookie_set_pool: AmazonCookieSetPool,
    event_queue: asyncio.Queue,
    event_loop_lock: asyncio.Lock,
    coroutine_id: uuid.UUID,
):
    logging.info(f"[coroutine_id={coroutine_id}]: Start cookie pool fill task")
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
        logging.info(f"[coroutine_id={coroutine_id}]: Event queue is full, waiting...")


async def _cookie_pool_process(
    cookie_set_pool: AmazonCookieSetPool,
    event_queue: asyncio.Queue,
    event_loop_lock: asyncio.Lock,
    coroutine_id: uuid.UUID,
):
    logging.info(f"[coroutine_id={coroutine_id}]: Start cookie pool process task")
    logging.info(f"[coroutine_id={coroutine_id}]: Queue size: {event_queue.qsize()}")
    try:
        msg = event_queue.get_nowait()
        fn, args = msg["fn"], msg["args"]
        asyncio.create_task(fn(**args))

    except asyncio.QueueEmpty:
        logging.info(f"[coroutine_id={coroutine_id}]: Event queue is empty, waiting...")


async def _cookie_pool_cleanup(
    cookie_set_pool: AmazonCookieSetPool,
    event_queue: asyncio.Queue,
    event_loop_lock: asyncio.Lock,
    coroutine_id: uuid.UUID,
):
    logging.info(f"[coroutine_id={coroutine_id}]: Start cookie pool cleanup task")
    logging.info(f"[coroutine_id={coroutine_id}]: Queue size: {event_queue.qsize()}")
    fn, args = start_cleanup_task, {
        "pool": cookie_set_pool,
        "coroutine_id": coroutine_id,
        "lock": event_loop_lock,
    }
    asyncio.create_task(fn(**args))


async def schedule_cookie_pool_fill(
    cookie_set_pool: AmazonCookieSetPool,
    event_queue: asyncio.Queue,
    event_loop_lock: asyncio.Lock,
    sleep_interval=12.2,
):
    logging.info("[MAIN]: Schedule cookie pool fill task")
    while True:
        coroutine_id = uuid.uuid4()
        await _cookie_pool_fill(
            cookie_set_pool, event_queue, event_loop_lock, coroutine_id
        )
        await asyncio.sleep(sleep_interval)


async def schedule_cookie_pool_process(
    cookie_set_pool: AmazonCookieSetPool,
    event_queue: asyncio.Queue,
    event_loop_lock: asyncio.Lock,
    sleep_interval=18.4,
):
    logging.info("[MAIN]: Schedule cookie pool process task")
    while True:
        coroutine_id = uuid.uuid4()
        await _cookie_pool_process(
            cookie_set_pool, event_queue, event_loop_lock, coroutine_id
        )
        await asyncio.sleep(sleep_interval)


async def schedule_cookie_pool_cleanup(
    cookie_set_pool: AmazonCookieSetPool,
    event_queue: asyncio.Queue,
    event_loop_lock: asyncio.Lock,
    sleep_interval=74.5,
):
    logging.info("[MAIN]: Schedule cookie pool cleanup task")
    while True:
        coroutine_id = uuid.uuid4()
        await _cookie_pool_cleanup(
            cookie_set_pool, event_queue, event_loop_lock, coroutine_id
        )
        await asyncio.sleep(sleep_interval)
