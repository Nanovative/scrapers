from enum import Enum


class BrowserType(str, Enum):
    firefox = "firefox"
    chromium = "chromium"


class ProxyType(str, Enum):
    static = "static"
    dynamic = "dynamic"
