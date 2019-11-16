import hashlib
import json
from typing import Any, Dict, Optional

import numpy as np

json_encoders = {np.ndarray: lambda v: v.flatten().tolist()}


def prepare_basis(basis: Optional[str]) -> Optional[str]:
    """
    Prepares a basis set string
    """
    if basis is None:
        return basis

    if basis == "":
        return None

    if basis == "null":
        return None

    return basis.lower()


def recursive_normalizer(value: Any, **kwargs: Dict[str, Any]) -> Any:
    """
    Prepare a structure for hashing by lowercasing all values and round all floats
    """
    digits = kwargs.get("digits", 10)
    lowercase = kwargs.get("lowercase", True)

    if isinstance(value, (int, type(None))):
        pass

    elif isinstance(value, str):
        if lowercase:
            value = value.lower()

    elif isinstance(value, list):
        value = [recursive_normalizer(x, **kwargs) for x in value]

    elif isinstance(value, tuple):
        value = tuple(recursive_normalizer(x, **kwargs) for x in value)

    elif isinstance(value, dict):
        ret = {}
        for k, v in value.items():
            if lowercase:
                k = k.lower()
            ret[k] = recursive_normalizer(v, **kwargs)
        value = ret

    elif isinstance(value, np.ndarray):
        if digits:
            # Round array
            value = np.around(value, digits)
            # Flip zeros
            value[np.abs(value) < 5 ** (-(digits + 1))] = 0

    elif isinstance(value, float):
        if digits:
            value = round(value, digits)
            if value == -0.0:
                value = 0
            if value == 0.0:
                value = 0

    else:
        raise TypeError("Invalid type in KeywordSet ({type(value)}), only simple Python types are allowed.")

    return value


def hash_dictionary(data: Dict[str, Any]) -> str:
    m = hashlib.sha1()
    m.update(json.dumps(data, sort_keys=True).encode("UTF-8"))
    return m.hexdigest()
