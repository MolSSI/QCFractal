from typing import Dict, List

from pydantic import Field

from qcportal.all_results import AllResultTypes
from qcportal.base_models import RestModelBase
from qcportal.managers import ManagerName


class TaskClaimBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    tags: List[str] = Field(..., description="Subset of tags to claim tasks from")
    limit: int = Field(..., description="Limit on the number of tasks to claim")


class TaskReturnBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    results: Dict[int, AllResultTypes]
