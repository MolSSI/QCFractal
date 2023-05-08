from datetime import datetime
from typing import Dict, List, Any, Optional

from pydantic import Field, BaseModel, constr, Extra

from qcportal.base_models import RestModelBase
from qcportal.compression import decompress, CompressionEnum
from qcportal.managers import ManagerName
from qcportal.record_models import PriorityEnum


class TaskClaimBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    programs: Dict[constr(to_lower=True), List[str]] = Field(..., description="Subset of programs to claim tasks for")
    tags: List[str] = Field(..., description="Subset of tags to claim tasks from")
    limit: int = Field(..., description="Limit on the number of tasks to claim")


class TaskReturnBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    results_compressed: Dict[int, bytes]


class TaskInformation(BaseModel):
    class Config(BaseModel.Config):
        extra = Extra.forbid

    id: int
    record_id: int
    required_programs: List[str]
    priority: PriorityEnum
    tag: str
    created_on: datetime

    function: str
    function_kwargs_compressed: Optional[bytes]

    @property
    def function_kwargs(self) -> Optional[Dict[str, Any]]:
        if self.function_kwargs_compressed is None:
            return None
        else:
            return decompress(self.function_kwargs_compressed, CompressionEnum.zstd)
