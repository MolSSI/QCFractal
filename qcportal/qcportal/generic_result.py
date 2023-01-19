from typing import Any, Optional, Dict

from qcelemental.models.common_models import Provenance, ComputeError, ProtoModel
from typing_extensions import Literal


class GenericTaskResult(ProtoModel):
    schema_name: Literal["qca_generic_task_result"] = "qca_generic_task_result"

    id: int
    results: Any

    stdout: Optional[str] = None
    stderr: Optional[str] = None

    success: bool
    provenance: Provenance
    extras: Dict[str, Any] = {}
    error: Optional[ComputeError] = None
