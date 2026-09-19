from enum import Enum

from pydantic import BaseModel, Field, field_validator, ConfigDict

from ..common_types import Max128Str
from ..exceptions import InvalidPasswordError, InvalidUsernameError, InvalidGroupnameError


class AuthTypeEnum(str, Enum):
    password = "password"


# Minimum length (in characters) required of a newly-set password
MIN_PASSWORD_LENGTH = 12

# bcrypt only considers the first 72 bytes of a password, and bcrypt >= 5.0 raises
# an error rather than silently truncating. So new passwords are capped here.
MAX_PASSWORD_BYTES = 72


def is_valid_password(password: str) -> None:
    """
    Checks that a password is acceptable as a *new* password

    This is the password policy applied when a password is being set (adding a user or
    changing a password). It is deliberately *not* applied when verifying a password at
    login time -- see the verification-time checks in the user socket -- since tightening
    this policy would otherwise lock out existing users with older, weaker passwords.

    Raises an InvalidPasswordError if the password is not acceptable.
    """

    if not isinstance(password, str):
        raise InvalidPasswordError("Password must be a string")

    # Null character not allowed
    if "\x00" in password:
        raise InvalidPasswordError("Password contains a NUL character")

    if len(password) == 0:
        raise InvalidPasswordError("Password is empty")

    # Password should be somewhat long
    if len(password) < MIN_PASSWORD_LENGTH:
        raise InvalidPasswordError(f"Password must contain at least {MIN_PASSWORD_LENGTH} characters")

    # ... but not longer than what bcrypt is able to handle
    if len(password.encode("UTF-8")) > MAX_PASSWORD_BYTES:
        raise InvalidPasswordError(
            f"Password must be at most {MAX_PASSWORD_BYTES} bytes when encoded as UTF-8 "
            "(note that non-ASCII characters take up more than one byte)"
        )


# Longest permitted username. Usernames are echoed into logs, error messages and JWT claims, and
# are accepted from unauthenticated callers at login, so an unbounded one is a way to make the
# server do unbounded work. This is generous compared to any real username
MAX_USERNAME_LENGTH = 64


def is_valid_username(username: str) -> None:
    if not isinstance(username, str):
        raise InvalidUsernameError("Username must be a string")

    if len(username) == 0:
        raise InvalidUsernameError("Username is empty")

    # Deliberately does not echo the username back - it is attacker-controlled at login
    if len(username) > MAX_USERNAME_LENGTH:
        raise InvalidUsernameError(f"Username must be at most {MAX_USERNAME_LENGTH} characters")

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

    id: int | None = None
    """ID of the group"""

    groupname: str
    """The name of the group"""

    description: str = ""
    """Text description of the group"""

    model_config = ConfigDict(extra="forbid", use_attribute_docstrings=True)

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
    id: int | None = Field(None, frozen=True)
    """The id of the user"""

    auth_type: AuthTypeEnum = Field(AuthTypeEnum.password, frozen=True)
    """Type of authentication the user uses"""

    username: str = Field(..., frozen=True)
    """The username of this user"""

    role: str
    """The role this user belongs to"""

    groups: list[str] = []
    """Groups this user belongs to"""

    enabled: bool
    """Whether this user is enabled or not"""

    fullname: Max128Str = ""
    """The full name or description of the user"""

    organization: Max128Str = ""
    """The organization the user belongs to"""

    email: Max128Str = ""
    """The email address for the user"""

    model_config = ConfigDict(extra="forbid", use_attribute_docstrings=True)

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
