from typing import Dict, List

try:
    from pydantic.v1 import Field, BaseModel, constr, Extra, validator, root_validator
except ImportError:
    from pydantic import Field, BaseModel, constr, Extra, validator, root_validator

from qcportal.base_models import RestModelBase
from qcportal.managers import ManagerName


class TaskClaimBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    programs: Dict[constr(to_lower=True), List[str]] = Field(..., description="Subset of programs to claim tasks for")
    compute_tags: List[str] = Field(..., description="Subset of tags to claim tasks from")
    limit: int = Field(..., description="Limit on the number of tasks to claim")

    @validator("compute_tags")
    def validate_tags(cls, v):
        v = [x for x in v if len(x) > 0]

        if len(v) == 0:
            raise ValueError("'tags' field contains no non-zero-length tags")

        return list(dict.fromkeys(v))  # remove duplicates, maintaining order (in python 3.6+)

    @validator("programs")
    def validate_programs(cls, v):
        # Remove programs of zero length
        v = {x: y for x, y in v.items() if len(x) > 0}
        if len(v) == 0:
            raise ValueError("'programs' field contains no non-zero-length programs")
        return v

    # TODO - DEPRECATED - remove at some point
    @root_validator(pre=True)
    def _old_tags(cls, values):
        if "tags" in values:
            values["compute_tags"] = values.pop("tags")

        return values


class TaskReturnBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    results_compressed: Dict[int, bytes]
