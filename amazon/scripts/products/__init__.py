import os
import sys

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(ROOT_PATH)

DEFAULT_DATA_DIR = f"{ROOT_PATH}/data/products"
DEFAULT_OUT_DIR = f"{ROOT_PATH}/out/products"
DEFAULT_LOG_DIR = f"{ROOT_PATH}/logs/products"


os.makedirs(DEFAULT_DATA_DIR, exist_ok=True, mode=0o777)
os.makedirs(DEFAULT_OUT_DIR, exist_ok=True, mode=0o777)
os.makedirs(DEFAULT_LOG_DIR, exist_ok=True, mode=0o777)
