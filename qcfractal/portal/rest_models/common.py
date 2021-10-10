from typing import Optional, List

from pydantic import BaseModel, validator


class DeleteParameters(BaseModel):
    id: Optional[List[int]] = None


class SimpleGetParameters(BaseModel):
    id: Optional[List[int]] = None
    missing_ok: Optional[bool] = False

    @validator("missing_ok", pre=True)
    def validate_lists(cls, v, field):
        if isinstance(v, list):
            # take the last value, if specified multiple times
            return v[-1]
        else:
            return v


class GetParameters(BaseModel):
    """
    Common query parameters to get functions
    """

    id: Optional[List[str]] = None
    include: Optional[List[str]] = None
    exclude: Optional[List[str]] = None
    missing_ok: Optional[bool] = False

    @validator("missing_ok", pre=True)
    def validate_lists(cls, v, field):
        if isinstance(v, list):
            # take the last value, if specified multiple times
            return v[-1]
        else:
            return v
