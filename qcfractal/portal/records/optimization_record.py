from typing import Any

from ...interface.models import OptimizationRecord as _OptimizationRecord
from .record import Record
from .record_utils import register_record


class OptimizationRecord(Record):
    """
    User-facing API for accessing data for a single optimization.

    """

    _DataModel = _OptimizationRecord
    _type = "optimization"

    def __init__(self, **kwargs: Any):
        """

        Parameters
        ----------
        **kwargs : Dict[str, Any]
            Additional keywords passed to the OptimizationRecord and the initial data constructor.
        """
        self._data = self._DataModel(**kwargs)

    @property
    def status(self):
        return self._data.status

    # TODO: add in optimization-specific methods


register_record(OptimizationRecord)
