from typing import Union

import base64
import numpy as np
import msgpack
import pydantic
from pydantic.json import pydantic_encoder
import json

from typing import Any


def _msgpack_encode(obj: Any) -> Any:
    try:
        return pydantic_encoder(obj)
    except TypeError:
        pass

    if isinstance(obj, np.ndarray):
        if obj.shape:
            return obj.ravel().tolist()
        else:
            return obj.tolist()

    return obj


def _msgpack_decode(obj: Any) -> Any:
    return obj


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        # JSON does not handle byte arrays
        # So convert to base64
        if isinstance(obj, bytes):
            return {"_bytes_base64_": base64.b64encode(obj).decode("ascii")}

        # Now do aything with pydantic
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


def _json_decode(obj):
    # Handle byte arrays
    if "_bytes_base64_" in obj:
        return base64.b64decode(obj["_bytes_base64_"].encode("ascii"))

    return obj


def deserialize(data: Union[bytes, str], content_type: str):
    if content_type == "application/msgpack":
        return msgpack.loads(data, object_hook=_msgpack_decode, raw=False)
    elif content_type == "application/json":

        # JSON stored as bytes? Decode into a string for json to load
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data, object_hook=_json_decode)
    else:
        raise RuntimeError(f"Unknown content type for deserialization: {content_type}")


def serialize(data, content_type: str):
    if content_type == "application/msgpack":
        return msgpack.dumps(data, default=_msgpack_encode, use_bin_type=True)
    elif content_type == "application/json":
        return json.dumps(data, cls=_JSONEncoder)
    else:
        raise RuntimeError(f"Unknown content type for serialization: {content_type}")
