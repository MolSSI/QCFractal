import base64
import json
from typing import Any, Type, TypeVar

import msgpack
import numpy as np
import pydantic
import pydantic_core

_V = TypeVar("_V")


def _msgpack_encode(obj: Any) -> Any:
    # msgpack can encode bytes natively. So don't dump to json-compatible
    # types here. But if we do that, we still need to handle numpy arrays

    # Flatten numpy arrays
    # This is mostly for Molecule class
    # TODO - remove once all data in the database in converted. This is probably only called serverside (with dicts)
    if isinstance(obj, np.ndarray):
        if obj.shape:
            return obj.ravel().tolist()
        else:
            return obj.tolist()

    return pydantic_core.to_jsonable_python(obj)


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        # JSON does not handle byte arrays
        # So convert to base64
        if isinstance(obj, bytes):
            return {"_bytes_base64_": base64.b64encode(obj).decode("ascii")}

        # Flatten numpy arrays
        # This is mostly for Molecule class
        # TODO - remove once all data in the database in converted. This is probably only called serverside (with dicts)
        if isinstance(obj, np.ndarray):
            if obj.shape:
                return obj.ravel().tolist()
            else:
                return obj.tolist()

        return pydantic_core.to_jsonable_python(obj)


def deserialize(data: bytes | str, content_type: str, model: Type[_V]) -> _V:
    if content_type.startswith("application/"):
        content_type = content_type[12:]

    if content_type == "msgpack":
        d = msgpack.loads(data, raw=False, strict_map_key=False)
        return pydantic.TypeAdapter(model).validate_python(d)
    elif content_type == "json":
        return pydantic.TypeAdapter(model).validate_json(data)
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
