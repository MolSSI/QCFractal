"""
Utility functions for QCPortal/QCFractal Interface.
"""
import re
import unicodedata

from pydantic import BaseModel

__all__ = ["replace_dict_keys", "normalize_filename"]


def replace_dict_keys(data, replacement):
    """
    Recursively replaces the keys in data from a dictionary `replacement`.
    """

    if isinstance(data, dict):
        ret = {}
        for k, v in data.items():

            # Replace key
            if k in replacement:
                k = replacement[k]

            # Recurse value if needed.
            new_v = v
            if isinstance(v, dict):
                new_v = replace_dict_keys(v, replacement)
            elif isinstance(v, (list, tuple)):
                new_v = [replace_dict_keys(x, replacement) for x in v]
                if isinstance(v, tuple):
                    new_v = tuple(new_v)

            ret[k] = new_v
        return ret

    elif isinstance(data, (tuple, list)):
        new_data = [replace_dict_keys(x, replacement) for x in data]
        if isinstance(data, tuple):
            new_data = tuple(new_data)

        return new_data

    elif isinstance(data, BaseModel):
        # Handle base model structures
        ret = data.copy()  # Create a copy
        search_keys = data.__fields__.keys()  # Enumerate keys
        for key in search_keys:
            existing_data = getattr(data, key)
            # Try to replace data recursively
            new_data = replace_dict_keys(existing_data, replacement)
            if new_data == existing_data:
                continue  # Do nothing if new data is the same (safer)
            setattr(ret, key, new_data)  # Replace new data in the copy to avoid in-place changes
        return ret  # Return

    else:
        return data


def normalize_filename(value: str) -> str:
    """
    Normalizes string to ASCII, removes non-alpha characters, and converts spaces to underscores.
    """
    value = str(value)
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip()
    return re.sub(r"[-\s]+", "_", value)
