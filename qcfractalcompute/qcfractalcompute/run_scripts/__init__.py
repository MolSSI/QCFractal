import os
from functools import lru_cache

_this_dir = os.path.abspath(os.path.dirname(__file__))


@lru_cache()
def get_script_path(name: str) -> str:
    return os.path.join(_this_dir, name)
