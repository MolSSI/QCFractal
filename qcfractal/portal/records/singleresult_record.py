from typing import Any

from ...interface.models import SingleResultRecord as _SingleResultRecord
from .record import Record
from .record_utils import register_record


class SingleResultRecord(Record):
    """
    User-facing API for accessing data for a single optimization.

    """
    _DataModel = _SingleResultRecord
    _type = "singleresult"
