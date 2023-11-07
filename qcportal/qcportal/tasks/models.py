from typing import Dict, List

try:
    from pydantic.v1 import Field, BaseModel, constr, Extra
except ImportError:
    from pydantic import Field, BaseModel, constr, Extra

from qcportal.base_models import RestModelBase
from qcportal.managers import ManagerName


class TaskClaimBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    programs: Dict[constr(to_lower=True), List[str]] = Field(..., description="Subset of programs to claim tasks for")
    tags: List[str] = Field(..., description="Subset of tags to claim tasks from")
    limit: int = Field(..., description="Limit on the number of tasks to claim")


class TaskReturnBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    results_compressed: Dict[int, bytes]
