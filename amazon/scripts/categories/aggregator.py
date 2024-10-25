import os
import uuid
import json
import logging

from shared.services.category_pool import CategoryPool
from shared.models.category import Category
from shared.storages.category.postgresql import PostgreSQLCategoryStorage

OUT_DIR = "./json"


def explore_with_depth(
    node: dict, name: str, path: str, parent: str, ancestor: str, depth: int = 0
):
    innerdata = {
        "name": name,
        "parent": parent,
        "ancestor": ancestor if depth > 0 else None,
        "is_leaf": False,
        "depth": depth,
        "url": node["url"],
        "path": path,
        "inner": {},
    }

    if not node["inner"]:
        innerdata["is_leaf"] = True
        return innerdata

    for category in node["inner"]:
        new_path = f"{path}/{category}"
        subdata = explore_with_depth(
            node["inner"][category],
            category,
            new_path,
            innerdata["name"],
            ancestor,
            depth + 1,
        )
        innerdata["inner"][category] = subdata
    return innerdata


def process_node(node: dict, processed: set):
    k = f"[depth={node['depth']}][name={node['name']}]"
    if k in processed:
        return []

    processed.add(k)

    if not node:
        return []

    inner = node["inner"]

    record = Category(**node)
    output = [record]

    for category in inner:
        innerdata = process_node(inner[category], processed)
        output += innerdata
    return output


def get_filenames():
    return [
        os.path.join(OUT_DIR, filename)
        for filename in os.listdir(OUT_DIR)
        if ".json" in filename
        and "\uf07c" not in filename
        and "processed" not in filename
        and "final" not in filename
    ]


async def aggregate():
    dict_data = {}
    lst_data = []
    processed = set()

    for old_filename in get_filenames():
        new_filename = old_filename.split(".json")[0] + ".processed.json"
        with open(old_filename, "r", encoding="utf-8") as read_file, open(
            new_filename, "w"
        ) as write_file:
            jsondata = json.load(read_file)
            category_name = jsondata["path"]
            processed_jsondata = explore_with_depth(
                jsondata,
                category_name,
                category_name,
                category_name,
                category_name,
            )
            dict_data[category_name] = processed_jsondata
            json.dump(
                processed_jsondata,
                write_file,
                indent=2,
            )

    with open(os.path.join(OUT_DIR, "final.json"), "w") as file:
        json.dump(dict_data, file, indent=2)

    for category_name in dict_data:
        ancestor = category_name
        node = dict_data[ancestor]
        lst_data += process_node(node, processed)

    storage = PostgreSQLCategoryStorage(os.getenv("POSTGRESQL_CONN_STR"))
    await storage.initialize()
    pool = CategoryPool(storage)

    num_of_records = len(lst_data)
    success = await pool.replace(lst_data, uuid.uuid4())

    if not success:
        logging.error(f"Failed to ingest {num_of_records} categories")

    else:
        logging.info(f"Successfully ingested {num_of_records} categories")
