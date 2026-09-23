import re
from datetime import datetime
from enum import Enum

from pydantic import AwareDatetime, BaseModel, Field, field_validator, ConfigDict

from ..common_types import Max128Str
from ..exceptions import InvalidPasswordError, InvalidUsernameError, InvalidGroupnameError

# Every API token begins with this fixed prefix. It makes a token recognizable to secret scanners
# and lets the server dispatch on the shape of an Authorization header before attempting to parse
# it as a JWT. Shared here so the client and server agree on it.
API_TOKEN_PREFIX = "qcf_"

# Longest name accepted for an API token
MAX_API_TOKEN_NAME_LENGTH = 128

# Longest string we will treat as a possible API token. A token is a fixed, known size; anything
# much larger is malformed, and (on the server) hashing an unbounded header on every request would
# be a cheap amplification vector.
MAX_API_TOKEN_LENGTH = 128

# The random part of a token is secrets.token_urlsafe output
_API_TOKEN_BODY_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class APITokenScopeEnum(str, Enum):
    """
    Named scopes for API tokens

    A token's scope is what it is allowed to do, relative to its owner's role - the effective
    permission can only ever be a restriction of the role, never an extension. A scope is stored
    and transmitted as a plain string, validated by validate_api_token_scope(); this enum lists
    the named values that validator accepts. Currently there is exactly one, ``unlimited``,
    meaning the token carries the owner's full role. Future scopes may be parameterized (for
    example, limiting writes to particular projects) and so will be handled by the validator's
    grammar rather than listed here. The "everything" scope is always this explicit named value -
    a null or empty scope must never be interpreted as unlimited access.
    """

    unlimited = "unlimited"


def validate_api_token_scope(scope: str | APITokenScopeEnum) -> str:
    """
    Validates an API token scope, returning its canonical string form

    This is the single place scope values are validated, shared by the client models and the
    server. Today the grammar is trivial - the only valid scope is "unlimited" - and future
    parameterized scopes extend the grammar here, without changing any field or column type.

    Raises ValueError for a scope this version does not recognize.
    """

    if isinstance(scope, APITokenScopeEnum):
        return scope.value
    if isinstance(scope, str):
        try:
            return APITokenScopeEnum(scope).value
        except ValueError:
            pass
    raise ValueError(f"Unknown API token scope: '{scope}'")


def looks_like_api_token(raw: str) -> bool:
    """
    Returns whether a string is shaped like an API token

    This is a cheap syntactic check (prefix, length, character set), shared by the client (to fail
    fast before a network round trip) and the server (to reject obviously-bad input before hashing).
    It says nothing about whether the token actually exists or is valid.
    """

    if not isinstance(raw, str) or not raw.startswith(API_TOKEN_PREFIX):
        return False
    if len(raw) > MAX_API_TOKEN_LENGTH:
        return False
    body = raw[len(API_TOKEN_PREFIX) :]
    return bool(body) and bool(_API_TOKEN_BODY_RE.match(body))


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


class APIToken(BaseModel):
    """
    Metadata about a long-lived API token

    This never contains the token itself. The plaintext token is shown exactly once, when the
    token is created (see NewAPIToken); afterwards only this metadata is available.
    """

    id: int
    """The id of the token (used to revoke it)"""

    user_id: int
    """The id of the user the token authenticates as"""

    token_prefix: str
    """The first few characters of the token, for identifying it in a listing"""

    name: str
    """The name given to the token when it was created (unique among the user's tokens)"""

    scope: str = APITokenScopeEnum.unlimited.value
    """What the token is allowed to do (see APITokenScopeEnum). Deliberately a plain string here,
    like UserInfo.role, so an older client can still parse listings from a newer server that has
    scopes this client does not know about"""

    created_at: datetime
    """When the token was created"""

    expires_at: datetime | None = None
    """When the token expires, or null if it never expires"""

    last_used_at: datetime | None = None
    """Approximate time the token was last presented on a request, or null if never used"""

    model_config = ConfigDict(extra="forbid", use_attribute_docstrings=True)


class NewAPIToken(BaseModel):
    """
    A newly-created API token, including the plaintext token

    The plaintext token is only available here, in the response to creating the token. It is not
    stored and cannot be retrieved later.
    """

    token: str
    """The plaintext token. Paste this into a client's Authorization header. Store it securely - it
    cannot be retrieved again"""

    info: APIToken
    """Metadata about the token"""

    model_config = ConfigDict(extra="forbid", use_attribute_docstrings=True)


class APITokenCreateBody(BaseModel):
    """
    Options for creating a new API token
    """

    name: str = Field(..., min_length=1, max_length=MAX_API_TOKEN_NAME_LENGTH)
    """A name to identify the token. Must be unique among the user's tokens."""

    scope: str = APITokenScopeEnum.unlimited.value
    """What the token should be allowed to do. Currently only "unlimited" (the owner's full role)
    exists. A plain (but validated) string, so the public schema never changes when new scopes
    are added; a scope this server does not recognize is rejected"""

    expires_at: AwareDatetime | None = None
    """When the token should expire. Must be timezone-aware. Null requests a non-expiring token,
    subject to the server's api_token_default_lifetime and api_token_max_lifetime policy"""

    model_config = ConfigDict(extra="forbid", use_attribute_docstrings=True)

    @field_validator("scope", mode="before")
    @classmethod
    def _valid_scope(cls, v):
        """Makes sure the scope is one this version understands"""

        try:
            return validate_api_token_scope(v)
        except Exception as e:
            raise ValueError(str(e))
