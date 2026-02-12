import base64
import json
from typing import Any

import msgpack
import numpy as np
from pydantic import BaseModel
from pydantic_core import to_jsonable_python


def _msgpack_encode(obj: Any) -> Any:
    if isinstance(obj, BaseModel):
        return obj.model_dump(exclude_unset=True, by_alias=True)

    if isinstance(obj, np.ndarray):
        if obj.shape:
            return obj.ravel().tolist()
        else:
            return obj.tolist()

    return to_jsonable_python(obj)

def _msgpack_decode(obj: Any) -> Any:
    return obj


def encode_to_json(obj: Any) -> Any:
    """
    Takes an object and turns it into plain python that can be encoded to JSON.

    This does not actually turn the object into JSON (string), just prepares it to be done.
    This is useful for turning various objects into something that can be put into a JSON(B) column
    in the database
    """

    # Basic types directly json serializable
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj

    # JSON does not handle byte arrays
    # So convert to base64
    if isinstance(obj, bytes):
        return {"_bytes_base64_": base64.b64encode(obj).decode("ascii")}

    # Basic types that JSON supports, but we need to convert elements
    if isinstance(obj, dict):
        return {k: encode_to_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [encode_to_json(v) for v in obj]

    # Now do anything with pydantic, excluding unset fields
    # Also always use aliases when serializing
    if isinstance(obj, BaseModel):
        return encode_to_json(obj.model_dump(mode="json", exclude_unset=True, by_alias=True))

    # Let pydantic handle other things
    try:
        return to_jsonable_python(obj)
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

    return obj


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        # JSON does not handle byte arrays
        # So convert to base64
        if isinstance(obj, bytes):
            return {"_bytes_base64_": base64.b64encode(obj).decode("ascii")}

        # Now do anything with pydantic
        # Include unset fields - discriminators might be there!
        # Also always use aliases when serializing
        if isinstance(obj, BaseModel):
            return obj.model_dump(by_alias=True)

        # Flatten numpy arrays
        # This is mostly for Molecule class
        # TODO - remove once all data in the database in converted
        if isinstance(obj, np.ndarray):
            if obj.shape:
                return obj.ravel().tolist()
            else:
                return obj.tolist()

        return to_jsonable_python(obj)


def _json_decode(obj):
    # Handle byte arrays
    if "_bytes_base64_" in obj:
        return base64.b64decode(obj["_bytes_base64_"].encode("ascii"))

    return obj


def deserialize(data: bytes | str, content_type: str):
    if content_type.startswith("application/"):
        content_type = content_type[12:]

    if content_type == "msgpack":
        return msgpack.loads(data, object_hook=_msgpack_decode, raw=False, strict_map_key=False)
    elif content_type == "json":
        # JSON stored as bytes? Decode into a string for json to load
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data, object_hook=_json_decode)
    else:
        raise RuntimeError(f"Unknown content type for deserialization: {content_type}")


def serialize(data, content_type: str) -> bytes:
    if content_type.startswith("application/"):
        content_type = content_type[12:]

    if content_type == "msgpack":
        return msgpack.dumps(data, default=_msgpack_encode, use_bin_type=True)
    elif content_type == "json":
        return json.dumps(data, cls=_JSONEncoder).encode("utf-8")
    else:
        raise RuntimeError(f"Unknown content type for serialization: {content_type}")


def convert_numpy_recursive(obj, flatten=False):
    if isinstance(obj, dict):
        return {k: convert_numpy_recursive(v, flatten) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [convert_numpy_recursive(v, flatten) for v in obj]
    elif isinstance(obj, np.ndarray):
        if obj.shape and flatten:
            return obj.ravel().tolist()
        else:
            return obj.tolist()
    else:
        return obj
