from typing import Optional, List

from pydantic import validator

from ..base_models import validate_list_to_single, RestModelBase


class UserGetParameters(RestModelBase):
    username: Optional[List[str]] = None
    missing_ok: Optional[bool] = False

    @validator("missing_ok", pre=True)
    def validate_lists(cls, v, field):
        return validate_list_to_single(v)


class RoleGetParameters(RestModelBase):
    rolename: Optional[List[str]] = None
    missing_ok: Optional[bool] = False

    @validator("missing_ok", pre=True)
    def validate_lists(cls, v):
        return validate_list_to_single(v)
