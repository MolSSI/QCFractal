from typing import Optional, List

from pydantic import BaseModel, validator


class UserGetParameters(BaseModel):
    username: Optional[List[str]] = None
    missing_ok: Optional[bool] = False

    # Query parameters can be specified multiple times. Therefore, we will always
    # convert them to a list in flask. But that means we have to convert them
    # to single values here
    @validator("missing_ok", pre=True)
    def validate_lists(cls, v, field):
        if isinstance(v, list):
            # take the last value, if specified multiple times
            return v[-1]
        else:
            return v


class RoleGetParameters(BaseModel):
    rolename: Optional[List[str]] = None
    missing_ok: Optional[bool] = False

    # Query parameters can be specified multiple times. Therefore, we will always
    # convert them to a list in flask. But that means we have to convert them
    # to single values here
    @validator("missing_ok", pre=True)
    def validate_lists(cls, v, field):
        if isinstance(v, list):
            # take the last value, if specified multiple times
            return v[-1]
        else:
            return v
