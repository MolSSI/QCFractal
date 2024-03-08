from __future__ import annotations

from enum import Enum
from typing import Optional, Union, List

try:
    from pydantic.v1 import BaseModel, Field, validator, constr, Extra
except ImportError:
    from pydantic import BaseModel, Field, validator, constr, Extra

from ..exceptions import InvalidPasswordError, InvalidUsernameError, InvalidRolenameError, InvalidGroupnameError


class AuthTypeEnum(str, Enum):
    password = "password"


def is_valid_password(password: str) -> None:
    # Null character not allowed
    if "\x00" in password:
        raise InvalidPasswordError("Password contains a NUL character")

    # Password should be somewhat long
    if len(password) == 0:
        raise InvalidPasswordError("Password is empty")

    if len(password) < 6:
        raise InvalidPasswordError("Password must contain at least 6 characters")


def is_valid_username(username: str) -> None:
    if len(username) == 0:
        raise InvalidUsernameError("Username is empty")

    # Null character not allowed
    if "\x00" in username:
        raise InvalidUsernameError("Username contains a NUL character")

    # Spaces are not allowed
    if " " in username:
        raise InvalidUsernameError("Username contains spaces")

    # Username cannot be all numbers
    if username.isnumeric():
        raise InvalidUsernameError("Username cannot be all numbers")


def is_valid_groupname(groupname: str) -> None:
    if len(groupname) == 0:
        raise InvalidGroupnameError("Groupname is empty")

    # Null character not allowed
    if "\x00" in groupname:
        raise InvalidGroupnameError("Groupname contains a NUL character")

    # Spaces are not allowed
    if " " in groupname:
        raise InvalidGroupnameError("Groupname contains spaces")

    # Groupname cannot be all numbers
    if groupname.isnumeric():
        raise InvalidGroupnameError("Groupname cannot be all numbers")


def is_valid_rolename(rolename: str) -> None:
    if len(rolename) == 0:
        raise InvalidRolenameError("Rolename is empty")

    # Null character not allowed
    if "\x00" in rolename:
        raise InvalidRolenameError("Rolename contains a NUL character")

    # Spaces are not allowed
    if " " in rolename:
        raise InvalidRolenameError("Rolename contains spaces")

    # Rolename cannot be all numbers
    if rolename.isnumeric():
        raise InvalidRolenameError("Rolename cannot be all numbers")


class PolicyStatement(BaseModel):
    """
    A statment of permissions for a role
    """

    Effect: str = Field(..., description="The effect of the permission (Allow, Deny)")
    Action: Union[str, List[str]] = Field(
        ..., description="The actions this permission applies to (GET, POST, etc). May be '*' to apply to all."
    )
    Resource: Union[str, List[str]] = Field(
        ...,
        description="The resource this permission applies to. Usually the first part of the path (molecules, "
        "keywords, etc). May be '*' to apply to all.",
    )


class PermissionsPolicy(BaseModel):
    """
    Permissions assigned to a role
    """

    class Config:
        extra = Extra.forbid

    Statement: List[PolicyStatement] = Field(..., description="Permission statements")


class RoleInfo(BaseModel):
    """
    Information about a role
    """

    class Config:
        extra = Extra.forbid

    rolename: str = Field(..., description="The name of the role")
    permissions: PermissionsPolicy = Field(..., description="The permissions associated with this role")

    @validator("rolename", pre=True)
    def _valid_rolename(cls, v):
        """Makes sure the username is a valid string"""

        try:
            is_valid_rolename(v)
            return v
        except Exception as e:
            raise ValueError(str(e))


class GroupInfo(BaseModel):
    """
    Information about a group
    """

    class Config:
        extra = Extra.forbid

    id: Optional[int] = Field(None, description="ID of the group")
    groupname: str = Field(..., description="The name of the group")
    description: str = Field("", description="Text description of the group")

    @validator("groupname", pre=True)
    def _valid_groupname(cls, v):
        """Makes sure the groupname is a valid string"""

        try:
            is_valid_groupname(v)
            return v
        except Exception as e:
            raise ValueError(str(e))


class UserInfo(BaseModel):
    """
    Information about a user
    """

    class Config:
        validate_assignment = True
        extra = Extra.forbid

    # id may be None when used for initial creation
    id: Optional[int] = Field(None, allow_mutation=False, description="The id of the user")
    auth_type: AuthTypeEnum = Field(
        AuthTypeEnum.password, allow_mutation=False, description="Type of authentication the user uses"
    )
    username: str = Field(..., allow_mutation=False, description="The username of this user")
    role: str = Field(..., description="The role this user belongs to")
    groups: List[str] = Field([], description="Groups this user belongs to")
    enabled: bool = Field(..., description="Whether this user is enabled or not")
    fullname: constr(max_length=128) = Field("", description="The full name or description of the user")
    organization: constr(max_length=128) = Field("", description="The organization the user belongs to")
    email: constr(max_length=128) = Field("", description="The email address for the user")

    @validator("username", pre=True)
    def _valid_username(cls, v):
        """Makes sure the username is a valid string"""

        try:
            is_valid_username(v)
            return v
        except Exception as e:
            raise ValueError(str(e))

    @validator("role", pre=True)
    def _valid_rolename(cls, v):
        """Makes sure the rolename is a valid string"""

        try:
            is_valid_rolename(v)
            return v
        except Exception as e:
            raise ValueError(str(e))

    @validator("groups")
    def _valid_groupnames(cls, v):
        """Makes sure the groupnames are valid strings"""

        try:
            for x in v:
                is_valid_groupname(x)
            return v
        except Exception as e:
            raise ValueError(str(e))
