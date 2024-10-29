import __init__

import os
import json
import logging
import argparse
import bs4

from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, wait


def data_checker(data_dir: str):
    names = (name for name in os.listdir(data_dir))
    deleted = 0

    for name in names:
        file_dir = f"{data_dir}/{name}"

        reason = "none"

        if name.find("redirect.txt") != -1:
            reason = "redirected"
        elif name.find("automation_detected.txt") != -1:
            reason = "automation detected"

        if reason != "none":
            logging.info(f"Invalid file: {file_dir} (reason = {reason})")
            deleted += 1

    logging.info(f"Found {deleted} invalid file(s)")


def process_txt_file(name: str, data_dir: str, out_dir: str, overwrite=False):
    txt_file_dir = f"{data_dir}/{name}"
    json_file_dir = f"{out_dir}/{name.replace('.txt', '.json')}"

    if name.find("redirect.txt") != -1:
        return (json_file_dir, False)

    if not overwrite and os.path.exists(json_file_dir):
        with open(json_file_dir, "r") as file:
            if file.read()[0] == "[":
                return (json_file_dir, True)
            else:
                logging.info(f"{json_file_dir} existed, skipping")
                return (json_file_dir, False)

    logging.info(f"TXT: Processing {name}")
    with open(txt_file_dir, "r") as file:
        txt_data = file.read()

    with open(json_file_dir, "w") as file:
        json_data = txt_data.replace("&&&", ",", txt_data.count("&&&") - 1).replace(
            "&&&", ""
        )
        json_data = json.loads(f"[{json_data}]")

        json.dump(json_data, file, indent=2)
        logging.info(f"TXT: Converted {txt_file_dir} to {json_file_dir}")

    return (json_file_dir, True)


def process_json_file(json_file_dir: str):
    logging.info(f"JSON: Processing {json_file_dir}")
    with open(json_file_dir, "r") as file:
        json_data = json.load(file)

    # Can replace with different name to produce different file instead of overwritting
    # Overwritting helps saving space for restricted environment (VM and containers)
    new_json_file_dir = json_file_dir.replace(".json", ".json")

    new_json_data = {
        "metadata": {},
        "data": {
            "count": 0,
            "items": [],
        },
    }
    for i, record in enumerate(json_data):
        if any(
            record[1].find(kw) != -1
            for kw in {"data-search-metadata", "data-main-slot:search-result"}
        ):
            k = "metadata"
            new_json_record = {
                "node_index": i,
                "node_key": record[1],
                **{f"item_{k}": v for k, v in record[2].items()},
            }
            if record[1] != "data-search-metadata":
                k = "data"
                data_node_contain_html = (
                    new_json_record.get("item_html", None) is not None
                )
                assert (
                    data_node_contain_html
                ), f"{new_json_file_dir}, {i}, {record[1]} doesn't contain HTML, which is very weird"

                new_json_record["contain_item_html"] = data_node_contain_html

            if k == "metadata":
                del new_json_record["item_html"]
                new_json_data[k] = new_json_record
            else:
                new_json_data[k]["count"] += 1
                new_json_data[k]["items"].append(new_json_record)

    for i, record in enumerate(new_json_data["data"]["items"]):
        parsed = bs4.BeautifulSoup(record["item_html"], "html.parser")
        if "seeing this ad based on the product" in parsed.text:
            new_json_data["data"]["items"][i] = None

        elif not new_json_data["data"]["items"][i]["contain_item_html"]:
            new_json_data["data"]["items"][i] = None

        else:
            uuid = None
            title = None
            url = None
            avg_rating = None
            total_review = None

            product_block = parsed.find("div", attrs={"data-uuid": True})
            if product_block:
                uuid = product_block.attrs.get("data-uuid")

            h2_tags = parsed.find("div", attrs={"data-cy": "title-recipe"}).find_all(
                "h2"
            )
            if len(h2_tags) > 1:
                h2_tags = h2_tags[1:]

            title_block = h2_tags[0]
            if title_block:
                title = title_block.get_text().strip()
                title_block = title_block.find("a")
                if title_block:
                    url = f"https://amazon.com/{title_block.attrs.get('href')}"

            review_block = parsed.find("div", attrs={"data-cy": "reviews-block"})

            if review_block:
                avg_rating_block = review_block.find(
                    "i", attrs={"data-cy": "reviews-ratings-slot"}
                )
                if avg_rating_block := review_block.find(
                    "i", attrs={"data-cy": "reviews-ratings-slot"}
                ):
                    avg_rating = avg_rating_block.get_text()
                    avg_rating = float(avg_rating.split("out of")[0].strip())

                if total_review_block := review_block.find(
                    "a", attrs={"class": ["a-size-base", "s-underline-text"]}
                ):
                    total_review = int(
                        total_review_block.get_text().replace(",", "").strip()
                    )

            del new_json_data["data"]["items"][i]["item_html"]
            new_json_data["data"]["items"][i] = {
                **new_json_data["data"]["items"][i],
                "item_uuid": uuid,
                "item_title": title,
                "item_url": url,
                "item_avg_rating": avg_rating,
                "item_total_review": total_review,
            }
            del new_json_data["data"]["items"][i]["contain_item_html"]

    new_json_data["data"]["items"] = [
        node for node in new_json_data["data"]["items"] if node is not None
    ]

    with open(new_json_file_dir, "w") as file:
        json.dump(new_json_data, file, indent=2)
        logging.info(f"JSON: Processed {json_file_dir}")

    return (json_file_dir, True)


def parse(name: str, data_dir: str, out_dir: str, overwrite=False):
    json_file_dir, success = process_txt_file(name, data_dir, out_dir, overwrite)
    if not success:
        return json_file_dir, False
    return process_json_file(json_file_dir)


def parse_txt_to_json(
    data_dir: str, out_dir: str, overwrite: bool, parse_count: int, skip_parse: int
):
    names = (name for name in sorted(os.listdir(data_dir)))
    batch_size = cpu_count()
    processed, unprocessed = [], []

    iterated = 0

    with ProcessPoolExecutor(max_workers=batch_size) as executor:
        futures = []
        for index, name in enumerate(names):
            if skip_parse >= 0 and index < skip_parse:
                continue

            if parse_count >= 0 and iterated >= parse_count:
                break

            if len(futures) >= batch_size:
                done, _ = wait(futures)
                for future in done:
                    filename, success = future.result()
                    if success:
                        processed.append(filename)
                    else:
                        unprocessed.append(filename)

                futures = []
                logging.info(f"Processed batch [{index - batch_size}:{index - 1}]")

            future = executor.submit(
                parse,
                name=name,
                data_dir=data_dir,
                out_dir=out_dir,
                overwrite=overwrite,
            )
            futures.append(future)
            iterated += 1

        if futures:
            done, _ = wait(futures)
            for future in done:
                filename, success = future.result()
                if success:
                    processed.append(filename)
                else:
                    unprocessed.append(filename)

    return processed, unprocessed


def main():
    parser = argparse.ArgumentParser(description="Process text files into JSON format.")
    parser.add_argument(
        "--data_dir",
        type=str,
        required=True,
        help="Directory containing text files.",
    )
    parser.add_argument(
        "--out_dir",
        type=str,
        required=True,
        help="Directory to save JSON files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing JSON files if they exist.",
    )
    parser.add_argument(
        "--parse_count",
        type=int,
        default=-1,
        help="Maximum number of records to parse",
    )
    parser.add_argument(
        "--skip_parse",
        type=int,
        default=-1,
        help="Number of records to skip",
    )

    args = parser.parse_args()

    # Execute filtering and parsing
    data_checker(args.data_dir)
    processed, _ = parse_txt_to_json(
        args.data_dir, args.out_dir, args.overwrite, args.parse_count, args.skip_parse
    )
