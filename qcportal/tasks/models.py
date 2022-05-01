from typing import Dict

from pydantic import Field

from qcportal.base_models import RestModelBase
from qcportal.managers import ManagerName
from qcportal.records import AllResultTypes


class TaskClaimBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    limit: int = Field(..., description="Limit on the number of tasks to claim")


class TaskReturnBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    results: Dict[int, AllResultTypes]
