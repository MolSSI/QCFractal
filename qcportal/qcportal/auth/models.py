from enum import Enum

from pydantic import BaseModel, Field, field_validator, ConfigDict

from ..common_types import Max128Str
from ..exceptions import InvalidPasswordError, InvalidUsernameError, InvalidGroupnameError


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
    if username.isdecimal():
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
    if groupname.isdecimal():
        raise InvalidGroupnameError("Groupname cannot be all numbers")


class GroupInfo(BaseModel):
    """
    Information about a group
    """

    id: int | None = Field(None, description="ID of the group")
    groupname: str = Field(..., description="The name of the group")
    description: str = Field("", description="Text description of the group")

    model_config = ConfigDict(extra="forbid")

    @field_validator("groupname", mode="before")
    @classmethod
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

    # id may be None when used for initial creation
    id: int | None = Field(None, frozen=True, description="The id of the user")
    auth_type: AuthTypeEnum = Field(
        AuthTypeEnum.password, frozen=True, description="Type of authentication the user uses"
    )
    username: str = Field(..., frozen=True, description="The username of this user")
    role: str = Field(..., description="The role this user belongs to")
    groups: list[str] = Field([], description="Groups this user belongs to")
    enabled: bool = Field(..., description="Whether this user is enabled or not")
    fullname: Max128Str = Field("", description="The full name or description of the user")
    organization: Max128Str = Field("", description="The organization the user belongs to")
    email: Max128Str = Field("", description="The email address for the user")

    model_config = ConfigDict(extra="forbid")

    @field_validator("username", mode="before")
    @classmethod
    def _valid_username(cls, v):
        """Makes sure the username is a valid string"""

        try:
            is_valid_username(v)
            return v
        except Exception as e:
            raise ValueError(str(e))

    @field_validator("groups", mode="after")
    @classmethod
    def _valid_groupnames(cls, v):
        """Makes sure the groupnames are valid strings"""

        try:
            for x in v:
                is_valid_groupname(x)
            return v
        except Exception as e:
            raise ValueError(str(e))
