import json
from filelock import FileLock
from typing import Any


def save_json_file(file_path: str, data: Any):
    lock_path = f"{file_path}.lock"
    with FileLock(lock_path):
        with open(file_path, "w") as file:
            json.dump(data, file, indent=2)


def load_json_file(file_path: str):
    lock_path = f"{file_path}.lock"
    with FileLock(lock_path):
        with open(file_path, "r") as file:
            return json.load(file)
