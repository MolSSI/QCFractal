from __future__ import annotations

from typing import Optional, Union, List, Any, TYPE_CHECKING

from pydantic import BaseModel, Field, validator, constr, Extra

from qcfractal.exceptions import InvalidPasswordError, InvalidUsernameError, InvalidRolenameError


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

    Statement: List[PolicyStatement] = Field(..., description="Permission statements")


class RoleInfo(BaseModel):
    """
    Information about a role
    """

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


class UserInfo(BaseModel):
    """
    Information about a user
    """

    class Config:
        validate_assignment = True
        extra = Extra.forbid

    # id may be None when used for initial creation
    id: Optional[int] = Field(None, allow_mutation=False, description="The id of the user")
    username: str = Field(..., allow_mutation=True, description="The username of this user")
    role: str = Field(..., description="The role this user belongs to")
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


class PortalRole(RoleInfo):
    client: Any  # TODO - circular reference to PortalClient

    @property
    def _url_base(self):
        return f"v1/role{self.rolename}"

    def update_on_server(self):
        updated = self.client._auto_request("put", self._url_base, RoleInfo, None, RoleInfo, RoleInfo.dict(self), None)

        for f in RoleInfo.__fields__:
            self.__dict__[f] = getattr(updated, f)


class PortalUser(UserInfo):
    client: Any  # TODO - circular reference to PortalClient
    as_admin: bool = False

    @property
    def is_current_user(self):
        return self.client.username == self.username

    @property
    def _url_base(self):
        # Access /user if we are trying to change another user, or if
        # we are explicitly trying to be an admin
        if self.as_admin or not self.is_current_user:
            return f"v1/user/{self.username}"
        else:
            return "v1/me"

    def update_on_server(self):
        updated = self.client._auto_request("put", self._url_base, UserInfo, None, UserInfo, UserInfo.dict(self), None)

        for f in UserInfo.__fields__:
            self.__dict__[f] = getattr(updated, f)

    def change_password(self, new_password: Optional[str]) -> str:
        """
        Changes a user's password on the server

        If `new_password` is None, then a new password will be generated and returned by this function.

        Parameters
        ----------
        new_password
            The new password for the user. If None, one will be generated

        Returns
        -------
        :
            If a new password was given, will return None. Otherwise,
        """

        # Check client-side and bail early if it's not a valid password
        if new_password is not None:
            is_valid_password(new_password)

        return self.client._auto_request(
            "put", f"{self._url_base}/password", Optional[str], None, str, new_password, None
        )

    def reset_password(self) -> str:
        """
        Resets a user's password, and returns the new password

        Equivalent to `change_password(None)`

        Returns
        -------
        :
            The newly-generated password for the user
        """

        return self.change_password(None)
