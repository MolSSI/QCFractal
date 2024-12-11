"""
Additional column types for SQLAlchemy
"""

try:
    from pydantic.v1 import BaseModel
    from pydantic.v1.json import pydantic_encoder
except ImportError:
    from pydantic import BaseModel
    from pydantic.json import pydantic_encoder

from typing import Any

import msgpack
import numpy as np
from sqlalchemy import TypeDecorator
from sqlalchemy.dialects.postgresql import BYTEA


def _msgpackext_encode(obj: Any) -> Any:
    # First try pydantic base objects
    try:
        return pydantic_encoder(obj)
    except TypeError:
        pass

    if isinstance(obj, np.ndarray):
        if obj.shape:
            data = {b"_nd_": True, b"dtype": obj.dtype.str, b"data": np.ascontiguousarray(obj).tobytes()}
            if len(obj.shape) > 1:
                data[b"shape"] = obj.shape
            return data

        else:
            # Converts np.array(5) -> 5
            return obj.tolist()

    return obj


def _msgpackext_decode(obj: Any) -> Any:
    if b"_nd_" in obj:
        arr = np.frombuffer(obj[b"data"], dtype=obj[b"dtype"])
        if b"shape" in obj:
            arr.shape = obj[b"shape"]

        return arr

    return obj


class MsgpackExt(TypeDecorator):
    """Converts JSON-like data to msgpack with full NumPy Array support."""

    impl = BYTEA

    # I believe caching is only used when, for example, you filter by a column. But we
    # shouldn't ever do that with msgpack
    cache_ok = False

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpack.dumps(value, default=_msgpackext_encode, use_bin_type=True)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpack.loads(value, object_hook=_msgpackext_decode, raw=False)


class PlainMsgpackExt(TypeDecorator):
    """Converts JSON-like data to msgpack using standard msgpack

    This does not support NumPy"""

    impl = BYTEA

    # I believe caching is only used when, for example, you filter by a column. But we
    # shouldn't ever do that with msgpack
    cache_ok = False

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpack.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpack.loads(value)
