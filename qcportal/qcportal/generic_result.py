from typing import Any, Literal

from pydantic import BaseModel

from .qcschema_v1 import Provenance, ComputeError


class GenericTaskResult(BaseModel):
    schema_name: Literal["qca_generic_task_result"] = "qca_generic_task_result"

    id: int
    results: Any

    stdout: str | None = None
    stderr: str | None = None

    success: bool
    provenance: Provenance
    extras: dict[str, Any] = {}
    error: ComputeError | None = None
