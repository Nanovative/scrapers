import sys
import uvicorn
import asyncio
import signal

from fastapi import FastAPI
from multiprocessing import cpu_count
from tasks import (
    schedule_cookie_pool_cleanup,
    schedule_cookie_pool_fill,
    schedule_cookie_pool_process,
)

CPU_COUNT = cpu_count()
MAX_WORKER = max(CPU_COUNT, 1)
COOKIE_POOL_WORKER = max(int(MAX_WORKER * 0.6), 1)
UVICORN_WORKER = max(MAX_WORKER - COOKIE_POOL_WORKER, 1)

print("background workers:", COOKIE_POOL_WORKER)
print("uvicorn workers:", UVICORN_WORKER)

shutdown_event = asyncio.Event()


# Run the application
async def run_api_app(event_loop: asyncio.AbstractEventLoop):
    from api.pool import router as pool_router
    from api.metadata import router as metadata_router

    cookie_pool_fill_task = event_loop.create_task(
        schedule_cookie_pool_fill(event_loop),
        name="cookie_pool_fill",
    )

    cookie_pool_process_task = event_loop.create_task(
        schedule_cookie_pool_process(event_loop),
        name="cookie_pool_process",
    )

    cookie_pool_cleanup_task = event_loop.create_task(
        schedule_cookie_pool_cleanup(event_loop),
        name="cookie_pool_cleanup",
    )

    app = FastAPI()
    app.include_router(pool_router)
    app.include_router(metadata_router)

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=8000,
        loop="asyncio",
        workers=UVICORN_WORKER,
    )
    server = uvicorn.Server(config)
    await server.serve()


def shutdown(signum, frame):
    print("Shutting down gracefully...")
    shutdown_event.set()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise NotImplementedError("API mode not specified")
    if sys.argv[1] == "api":
        # Setup signal handling
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(event_loop)

        try:
            while not shutdown_event.is_set():
                event_loop.run_until_complete(run_api_app(event_loop))
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            event_loop.stop()
            event_loop.close()
    else:
        raise NotImplementedError("Invalid argument")
