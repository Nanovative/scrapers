import sys
import uvicorn
import asyncio
import aiocron
import signal
from multiprocessing import cpu_count
from service import start_add_task, start_cleanup_task
from model import event_loop_lock

CPU_COUNT = cpu_count()
MAX_WORKER = max(CPU_COUNT, 1)
COOKIE_POOL_WORKER = max(int(MAX_WORKER * 0.6), 1)
UVICORN_WORKER = max(MAX_WORKER - COOKIE_POOL_WORKER, 1)


# Create a new event loop
event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(event_loop)

shutdown_event = asyncio.Event()

print("background workers:", COOKIE_POOL_WORKER)
print("uvicorn workers:", UVICORN_WORKER)

@aiocron.crontab("* * * * *", loop=event_loop)
async def schedule_cookie_pool_fill():
    print("Start cookie pool fill task")
    for i in range(COOKIE_POOL_WORKER):
        event_loop.create_task(start_add_task(event_loop_lock, False))


@aiocron.crontab("*/2 * * * *", loop=event_loop)
async def schedule_cookie_pool_cleanup():
    print("Start cookie pool cleanup task")
    event_loop.create_task(start_cleanup_task(event_loop_lock))


# Run the application
async def run_app():
    config = uvicorn.Config(
        "api:app",
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

        try:
            while not shutdown_event.is_set():
                event_loop.run_until_complete(run_app())
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            event_loop.stop()
            event_loop.close()
    else:
        raise NotImplementedError("Invalid argument")
