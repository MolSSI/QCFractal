from typing import Literal, Any

from qcportal.record_models import BaseRecord


class ServiceSubtaskRecord(BaseRecord):
    record_type: Literal["servicesubtask"] = "servicesubtask"

    required_programs: dict[str, Any]
    function: str
    function_kwargs: dict[str, Any]
    results: Any
