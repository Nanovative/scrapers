import __init__

import sys
import uvicorn
import asyncio
import signal
import logging

from shared.config.logger import safely_start_logger
from threading import Thread

from fastapi import FastAPI
from multiprocessing import cpu_count
from shared.tasks import (
    schedule_cookie_pool_cleanup,
    schedule_cookie_pool_fill,
    schedule_cookie_pool_process,
)
from shared.utils import (
    event_queue,
    event_loop_lock,
    get_cookie_set_pool,
    get_category_pool,
    get_proxy_pool,
)

shutdown_event = asyncio.Event()


# Run the application
async def run_api_app(num_workers: int = None):
    from routes.cookie import router as cookie_router
    from routes.proxy import router as proxy_router
    from routes.metadata import router as metadata_router
    from routes.category import router as category_router

    await get_category_pool()
    await get_proxy_pool()
    cookie_set_pool = await get_cookie_set_pool()

    cookie_pool_fill_task = asyncio.create_task(
        schedule_cookie_pool_fill(cookie_set_pool, event_queue, event_loop_lock),
        name="cookie_pool_fill",
    )

    cookie_pool_process_task = event_loop.create_task(
        schedule_cookie_pool_process(cookie_set_pool, event_queue, event_loop_lock),
        name="cookie_pool_process",
    )

    cookie_pool_cleanup_task = event_loop.create_task(
        schedule_cookie_pool_cleanup(cookie_set_pool, event_queue, event_loop_lock),
        name="cookie_pool_cleanup",
    )

    app = FastAPI()

    app.include_router(metadata_router, prefix="/meta")
    app.include_router(cookie_router, prefix="/cookie")
    app.include_router(proxy_router, prefix="/proxy")
    app.include_router(category_router, prefix="/category")

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8000,
        loop="asyncio",
        workers=num_workers,
    )
    server = uvicorn.Server(config)
    logging.info(
        f"Starting FastAPI server with {num_workers if num_workers is not None else 'unknown'} workers"
    )
    await server.serve()


def shutdown(signum, frame):
    logging.info("Shutting down gracefully...")
    shutdown_event.set()


def run_event_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    background_loop = asyncio.new_event_loop()
    thread = Thread(target=run_event_loop, args=(background_loop,), daemon=True)
    thread.start()

    future = asyncio.run_coroutine_threadsafe(safely_start_logger(), background_loop)

    # Setup signal handling
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)

    # I calculated this for fun
    # MAX_WORKER = max(cpu_count(), 1)
    # COOKIE_POOL_WORKER = max(int(MAX_WORKER * 0.6), 1)
    # UVICORN_WORKER = max(MAX_WORKER - COOKIE_POOL_WORKER, 1)

    try:
        while not shutdown_event.is_set():
            event_loop.run_until_complete(run_api_app())
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    finally:
        event_loop.stop()
        event_loop.close()
