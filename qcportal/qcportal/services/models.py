from __future__ import annotations

from typing import Dict, Any

from typing_extensions import Literal

from qcportal.record_models import BaseRecord


class ServiceSubtaskRecord(BaseRecord):
    record_type: Literal["servicesubtask"] = "servicesubtask"

    required_programs: Dict[str, Any]
    function: str
    function_kwargs: Dict[str, Any]
    results: Any
