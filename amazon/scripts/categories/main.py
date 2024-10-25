import __init__

import sys
import math
import logging
import asyncio
import threading

from shared.config.logger import safely_start_logger
from scraper import scrape_category_tree
from aggregator import aggregate


def run_event_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def execute_pipeline(argv: list[str]):
    assert (
        len(argv) >= 5
    ), "Missing (is_headless, browser_type, batch_size, batch_index)"
    keywords = [
        "Amazon Devices",
        "Appliances",
        "Automotive Parts & Accessories",
        "Beauty & Personal Care",
        "Cell Phones & Accessories",
        "Computers",
        "Electronics",
        "Grocery & Gourmet Food",
        "Health, Household & Baby Care",
        "Pet Supplies",
        "Premium Beauty",
        "Smart Home",
    ]
    num_of_keywords = len(keywords)
    assert num_of_keywords > 0, "List of explorable keywords is empty"

    is_headless: bool = argv[1] if argv[1].lower() == "true" else False
    browser_type: str = argv[2]

    batch_size = int(argv[3])
    assert (
        batch_size and batch_size <= num_of_keywords
    ), f"Invalid batch size (must be between 1 and {num_of_keywords})"

    max_batch_num = math.ceil(num_of_keywords / batch_size) - 1
    batch_num = int(argv[4])
    assert (
        batch_num >= 0 and batch_num <= max_batch_num
    ), f"Invalid batch num (must be between 0 and {max_batch_num})"

    batch_start_idx, batch_end_idx = (batch_num * batch_size), (
        (batch_num + 1) * batch_size
    )
    assert (
        batch_start_idx < num_of_keywords
    ), f"Invalid batch index [{batch_start_idx}:{batch_end_idx}]"

    keywords_to_process = keywords[batch_start_idx:batch_end_idx]

    logging.info(f"Executing pipeline to scrape category tree of {keywords_to_process}")

    asyncio.run(
        scrape_category_tree(
            is_headless=is_headless,
            browser_type_str=browser_type,
            keywords=keywords_to_process,
        )
    )


def execute_aggregate(argv: list[str]):
    asyncio.run(aggregate())


if __name__ == "__main__":
    # setup logger
    background_loop = asyncio.new_event_loop()
    thread = threading.Thread(
        target=run_event_loop, args=(background_loop,), daemon=True
    )
    thread.start()
    future = asyncio.run_coroutine_threadsafe(safely_start_logger(), background_loop)

    assert len(sys.argv) >= 2, "Missing method (scrape, aggregate)"
    argv = sys.argv[1:]
    if sys.argv[1] == "aggregate":
        execute_aggregate(argv)
    elif sys.argv[1] == "scrape":
        # run pipeline
        execute_pipeline(argv)
    else:
        raise NotImplementedError("Must be (scrape, aggregate)")
