import base64
import json
from hashlib import sha256
from typing import Any

import numpy as np

try:
    from pydantic.v1 import BaseModel
    from pydantic.v1.json import pydantic_encoder
except ImportError:
    from pydantic import BaseModel
    from pydantic.json import pydantic_encoder


class _JSONEncoder_1(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        # JSON does not handle byte arrays
        # So convert to base64
        if isinstance(obj, bytes):
            return {"_bytes_base64_": base64.b64encode(obj).decode("ascii")}

        # Now do anything with pydantic, excluding unset fields
        # Also always use aliases when serializing
        if isinstance(obj, BaseModel):
            return obj.dict(exclude_unset=True, by_alias=True)

        # Let pydantic handle other things
        try:
            return pydantic_encoder(obj)
        except TypeError:
            pass

        # Flatten numpy arrays
        # This is mostly for Molecule class
        # TODO - remove once all data in the database in converted
        if isinstance(obj, np.ndarray):
            if obj.shape:
                return obj.ravel().tolist()
            else:
                return obj.tolist()

        return json.JSONEncoder.default(self, obj)


def hash_dict_1(d):
    j = json.dumps(d, ensure_ascii=True, sort_keys=True, cls=_JSONEncoder_1).encode("utf-8")
    return sha256(j).hexdigest()
