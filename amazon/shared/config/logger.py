import sys
import asyncio
import logging
import threading

from queue import SimpleQueue
from shared.utils import run_event_loop
from logging.handlers import QueueHandler, QueueListener

# Initialize a queue for log records (helps with async and thread-safety)
log_queue = SimpleQueue()

# Create the logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# Reference to the logger task
LOGGER_TASK: asyncio.Task = None

# Handler for stdout (INFO and DEBUG)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.DEBUG)
stdout_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
stdout_handler.setFormatter(stdout_format)

# Handler for stderr (WARNING and above)
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.WARNING)
stderr_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
stderr_handler.setFormatter(stderr_format)

# Apply filters if using a single logger approach
stdout_handler.addFilter(lambda record: record.levelno < logging.WARNING)
stderr_handler.addFilter(lambda record: record.levelno >= logging.WARNING)


async def init_logger():
    # Queue handlers for thread and async safety
    queue_listener = QueueListener(
        log_queue,
        stdout_handler,
        stderr_handler,
    )
    logger.addHandler(QueueHandler(log_queue))

    try:
        # start the listener
        queue_listener.start()
        # report the logger is ready
        logging.debug(f"Logger has started")
        # wait forever
        while True:
            await asyncio.sleep(60)
    finally:
        # report the logger is done
        logging.debug(f"Logger is shutting down")
        # ensure the listener is closed
        queue_listener.stop()


# coroutine to safely start the logger
async def safely_start_logger():
    global LOGGER_TASK
    # initialize the logger
    logging.debug("Attempting to start logger")
    if LOGGER_TASK is None or not LOGGER_TASK.done():
        LOGGER_TASK = asyncio.create_task(init_logger())
    # allow the logger to start
    await asyncio.sleep(0)


def setup_logger():
    # Setup logger
    background_loop = asyncio.new_event_loop()
    thread = threading.Thread(
        target=run_event_loop, args=(background_loop,), daemon=True
    )
    thread.start()
    future = asyncio.run_coroutine_threadsafe(safely_start_logger(), background_loop)
    future.result()
