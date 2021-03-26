from typing import Any

from ...interface.models import OptimizationRecord as _OptimizationRecord


class OptimizationRecord:
    """
    User-facing API for accessing data for a single optimization.

    """

    _DataModel = _OptimizationRecord

    def __init__(self, **kwargs: Any):
        """

        Parameters
        ----------
        **kwargs : Dict[str, Any]
            Additional keywords passed to the OptimizationRecord and the initial data constructor.
        """
        # Create the data model
        self._data = self._DataModel(**kwargs)


register_record(OptimizationRecord)
