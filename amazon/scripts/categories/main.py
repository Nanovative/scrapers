import __init__

import sys
import math
import logging
import asyncio
import threading
import argparse

from shared.config.logger import safely_start_logger
from scraper import scrape_category_tree
from aggregator import aggregate


def run_event_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def execute_pipeline(
    is_headless: bool, browser_type: str, batch_size: int, batch_num: int
):
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
        "Baby",
        "Toys & Games",
        "Video Games",
    ]
    num_of_keywords = len(keywords)
    if num_of_keywords == 0:
        raise ValueError("List of explorable keywords is empty")

    if batch_size <= 0 or batch_size > num_of_keywords:
        raise ValueError(
            f"Invalid batch size (must be between 1 and {num_of_keywords})"
        )

    max_batch_num = math.ceil(num_of_keywords / batch_size) - 1
    if batch_num < 0 or batch_num > max_batch_num:
        raise ValueError(f"Invalid batch num (must be between 0 and {max_batch_num})")

    batch_start_idx = batch_num * batch_size
    batch_end_idx = (batch_num + 1) * batch_size
    if batch_start_idx >= num_of_keywords:
        raise ValueError(f"Invalid batch index [{batch_start_idx}:{batch_end_idx}]")

    keywords_to_process = keywords[batch_start_idx:batch_end_idx]

    logging.info(f"Executing pipeline to scrape category tree of {keywords_to_process}")

    asyncio.run(
        scrape_category_tree(
            is_headless=is_headless,
            browser_type_str=browser_type,
            keywords=keywords_to_process,
        )
    )


def execute_aggregate():
    asyncio.run(aggregate())


if __name__ == "__main__":
    # setup logger
    background_loop = asyncio.new_event_loop()
    thread = threading.Thread(
        target=run_event_loop, args=(background_loop,), daemon=True
    )
    thread.start()
    future = asyncio.run_coroutine_threadsafe(safely_start_logger(), background_loop)
    future.result()

    # Set up argparse
    parser = argparse.ArgumentParser(description="Scrape categories or aggregate data.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subparser for scrape
    scrape_parser = subparsers.add_parser(
        "scrape", help="Scrape categories from the source."
    )
    scrape_parser.add_argument(
        "--is_headless", type=bool, default=True, help="Run browser in headless mode."
    )
    scrape_parser.add_argument(
        "--browser_type", type=str, required=True, help="Type of browser to use."
    )
    scrape_parser.add_argument(
        "--batch_size", type=int, required=True, help="Number of keywords per batch."
    )
    scrape_parser.add_argument(
        "--batch_num", type=int, required=True, help="Batch index to process."
    )

    # Subparser for aggregate
    aggregate_parser = subparsers.add_parser(
        "aggregate", help="Aggregate scraped data."
    )

    # Parse arguments
    args = parser.parse_args()

    if args.command == "aggregate":
        execute_aggregate()
    elif args.command == "scrape":
        execute_pipeline(
            args.is_headless, args.browser_type, args.batch_size, args.batch_num
        )
