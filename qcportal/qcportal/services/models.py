from __future__ import annotations

from typing import Dict, Any, Optional

from typing_extensions import Literal

from qcportal.record_models import BaseRecord


class ServiceSubtaskRecord(BaseRecord):
    class _DataModel(BaseRecord._DataModel):
        record_type: Literal["servicesubtask"] = "servicesubtask"

        required_programs: Dict[str, Any]
        function: str
        function_kwargs_lb_id: Optional[int]
        results_lb_id: Optional[int]

    raw_data: ServiceSubtaskRecord._DataModel

    @property
    def required_programs(self) -> Dict[str, Any]:
        return self.raw_data.required_programs

    @property
    def function(self) -> str:
        return self.raw_data.function
