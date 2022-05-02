"""
Additional column types for SQLAlchemy
"""

import msgpack
from qcelemental.util import msgpackext_dumps, msgpackext_loads
from sqlalchemy import TypeDecorator
from sqlalchemy.dialects.postgresql import BYTEA


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
            return msgpackext_dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return msgpackext_loads(value)


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
