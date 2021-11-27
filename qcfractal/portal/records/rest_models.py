from datetime import datetime
from typing import Optional, List

from pydantic import validator

from qcfractal.portal.common_rest import QueryProjParametersBase, RestModelBase, validate_list_to_single
from .models import RecordStatusEnum, PriorityEnum


class RecordModifyBody(RestModelBase):
    record_id: Optional[List[int]] = None
    status: Optional[RecordStatusEnum] = None
    priority: Optional[PriorityEnum]
    tag: Optional[str] = None
    delete_tag: bool = False


class RecordQueryBody(QueryProjParametersBase):
    id: Optional[List[int]] = None
    record_type: Optional[List[str]] = None
    manager_name: Optional[List[str]] = None
    status: Optional[List[RecordStatusEnum]] = None
    created_before: Optional[datetime] = None
    created_after: Optional[datetime] = None
    modified_before: Optional[datetime] = None
    modified_after: Optional[datetime] = None


class ComputeHistoryURLParameters(RestModelBase):
    """
    URL parameters for obtaining compute history for a record
    """

    include_outputs: Optional[bool] = False

    @validator("include_outputs", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)
