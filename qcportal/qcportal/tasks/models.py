from pydantic import Field, model_validator, field_validator

from qcportal.base_models import RestModelBase
from qcportal.common_types import LowerStr, QCPortalBytes
from qcportal.managers import ManagerName


class TaskClaimBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    programs: dict[LowerStr, list[str]] = Field(..., description="Subset of programs to claim tasks for")
    compute_tags: list[str] = Field(..., description="Subset of tags to claim tasks from")
    limit: int = Field(..., description="Limit on the number of tasks to claim")

    @field_validator("compute_tags", mode="after")
    @classmethod
    def validate_tags(cls, v):
        v = [x for x in v if len(x) > 0]

        if len(v) == 0:
            raise ValueError("'tags' field contains no non-zero-length tags")

        return list(dict.fromkeys(v))  # remove duplicates, maintaining order (in python 3.6+)

    @field_validator("programs", mode="after")
    @classmethod
    def validate_programs(cls, v):
        # Remove programs of zero length
        v = {x: y for x, y in v.items() if len(x) > 0}
        if len(v) == 0:
            raise ValueError("'programs' field contains no non-zero-length programs")
        return v

    @model_validator(mode="before")
    @classmethod
    def _old_tags(cls, values):
        if isinstance(values, dict):
            if "tags" in values:
                values["compute_tags"] = values.pop("tags")
        return values


class TaskReturnBody(RestModelBase):
    name_data: ManagerName = Field(..., description="Name information about this manager")
    results_compressed: dict[int, QCPortalBytes]
