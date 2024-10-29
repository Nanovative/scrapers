import __init__
from __init__ import DEFAULT_DATA_DIR, DEFAULT_OUT_DIR

import argparse
import asyncio
import preprocessor
import scraper

from shared.config.logger import setup_logger


def preprocess(args):
    # Process text files to JSON
    preprocessor.data_checker(args.data_dir)
    preprocessor.parse_txt_to_json(
        args.data_dir,
        args.out_dir,
        args.overwrite,
        args.parse_count,
        args.skip_parse,
    )


def scrape(args):
    # Start the scraping process
    asyncio.run(
        scraper.execute_pipeline(
            start=args.page_start,
            end=args.page_end,
            step=args.batch_size,
            stop=args.max_page_per_category,
            depth=args.max_depth,
            overwrite=args.overwrite,
        )
    )


def main():
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Process text files into JSON format and scrape categories."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand for preprocessing
    preprocess_parser = subparsers.add_parser(
        "preprocess", help="Process text files into JSON."
    )
    preprocess_parser.add_argument(
        "--data_dir",
        type=str,
        default=DEFAULT_DATA_DIR,
        help="Directory containing text files.",
    )
    preprocess_parser.add_argument(
        "--out_dir",
        type=str,
        default=DEFAULT_OUT_DIR,
        help="Directory to save JSON files.",
    )
    preprocess_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing JSON files if they exist.",
    )
    preprocess_parser.add_argument(
        "--parse_count",
        type=int,
        default=-1,
        help="Maximum number of records to parse",
    )
    preprocess_parser.add_argument(
        "--skip_parse",
        type=int,
        default=-1,
        help="Number of records to skip",
    )

    # Subcommand for scraping
    scrape_parser = subparsers.add_parser(
        "scrape", help="Scrape categories from a source."
    )
    scrape_parser.add_argument(
        "--page_start", type=int, default=1, help="Starting page for scraping."
    )
    scrape_parser.add_argument(
        "--page_end", type=int, default=45, help="Ending page for scraping."
    )
    scrape_parser.add_argument(
        "--max_depth", type=int, default=2, help="Maximum depth for scraping."
    )
    scrape_parser.add_argument(
        "--batch_size", type=int, default=15, help="Batch size for scraping."
    )
    scrape_parser.add_argument(
        "--max_page_per_category",
        type=int,
        default=None,
        help="Maximum pages per category.",
    )
    scrape_parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    # Setup logger
    setup_logger()

    # Execute the appropriate command
    if args.command == "preprocess":
        preprocess(args)
    elif args.command == "scrape":
        scrape(args)


if __name__ == "__main__":
    main()
