from typing import Dict, List, Any, Optional

from pydantic import Field, BaseModel, constr

from qcportal.all_results import AllResultTypes
from qcportal.base_models import RestModelBase
from qcportal.managers import ManagerName
from qcportal.record_models import PriorityEnum


class TaskClaimBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    programs: Dict[constr(to_lower=True), List[str]] = Field(..., description="Subset of programs to claim tasks for")
    tags: List[str] = Field(..., description="Subset of tags to claim tasks from")
    limit: int = Field(..., description="Limit on the number of tasks to claim")


class TaskReturnBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    results: Dict[int, AllResultTypes]


class TaskInformation(BaseModel):
    id: int
    record_id: int
    required_programs: List[str]
    priority: PriorityEnum
    tag: str

    function: str
    function_kwargs: Dict[str, Any]
    function_kwargs_lbid: Optional[int]
