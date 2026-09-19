from __future__ import annotations

from typing import TYPE_CHECKING

import bcrypt
import pytest
from sqlalchemy import select
from qcfractal.components.auth.db_models import UserORM
from qcfractal.components.auth.user_socket import BCRYPT_COST, _bcrypt_cost, _hash_password
from qcportal.auth.models import UserInfo, GroupInfo, AuthTypeEnum, is_valid_password
from qcportal.exceptions import (
    UserManagementError,
    AuthenticationFailure,
    InvalidPasswordError,
    InvalidUsernameError,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket

invalid_usernames = ["\x00", "ab\x00cd", "a user", "", "u" * 65, "u" * 100000]

# Passwords that are not acceptable as *new* passwords (that is, violate the password policy
# enforced when adding a user or changing a password)
invalid_passwords = [
    "\x00",
    "abcd\x00efgh",
    "abcd",
    "1",
    "",
    "abcdefghijk",  # 11 characters - one short of the minimum
    "a" * 73,  # too long for bcrypt
    "é" * 37,  # 37 characters, but 74 bytes when encoded as UTF-8
]

# Passwords that can't even be checked against a stored hash. Note that this is a much shorter
# list than the above - an existing user with a short password must still be able to log in
invalid_verify_passwords = ["\x00", "abcd\x00efgh", ""]


def _seed_user_with_hash(storage_socket: SQLAlchemySocket, username: str, hashed_password: bytes) -> int:
    """
    Adds a user directly through the ORM, with a pre-computed password hash

    This bypasses the password policy & hashing done in the user socket, and is used to
    emulate users that were created by older versions of QCFractal.
    """

    with storage_socket.session_scope() as session:
        user = UserORM(
            username=username,
            role="read",
            auth_type=AuthTypeEnum.password,
            enabled=True,
            password=hashed_password,
        )
        session.add(user)
        session.flush()
        return user.id


def _get_stored_hash(storage_socket: SQLAlchemySocket, username: str) -> bytes:
    """
    Reads a user's stored password hash directly from the database, using a brand-new session
    """

    with storage_socket.session_scope(True) as session:
        user = session.execute(select(UserORM).where(UserORM.username == username)).scalar_one()
        return user.password


def test_user_socket_add_get(storage_socket: SQLAlchemySocket):
    storage_socket.groups.add(GroupInfo(groupname="group1"))
    storage_socket.groups.add(GroupInfo(groupname="group2"))

    uinfo = UserInfo(
        username="george",
        role="read",
        groups=["group1", "group2"],
        enabled=True,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    pw = storage_socket.users.add(uinfo, password="old_password_123")
    assert pw == "old_password_123"

    # Do we get the same data back?
    # The initial userinfo doesn't contain the id
    uinfo2 = storage_socket.users.get("george")
    assert "password" not in uinfo2
    uinfo2.pop("id")
    assert UserInfo(**uinfo2) == uinfo

    # Get by id
    uinfo2 = storage_socket.users.get("george")
    uinfo3 = storage_socket.users.get(uinfo2["id"])
    assert uinfo2 == uinfo3


def test_user_socket_add_duplicate(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo, password="old_password_123")

    # Duplicate should result in an exception
    uinfo2 = UserInfo(username="george", role="read", enabled=True)
    with pytest.raises(UserManagementError, match=r"User.*already exists"):
        storage_socket.users.add(uinfo2, "new_password_123")


def test_user_socket_add_with_id(storage_socket: SQLAlchemySocket):
    # Should not be able to add a user with the id set
    uinfo = UserInfo(
        id=123,
        username="george",
        role="read",
        enabled=True,
    )

    with pytest.raises(UserManagementError, match=r"id was given as part"):
        storage_socket.users.add(uinfo, password="old_password_123")


def test_user_socket_list(storage_socket: SQLAlchemySocket):
    all_users = []
    for i in range(20):
        uinfo = UserInfo(
            username=f"george_{i}",
            role="read",
            enabled=bool(i % 2),
            fullname=f"Test user_{i}",
            email=f"george{i}@example.com",
            organization=f"My Org {i}",
        )
        storage_socket.users.add(uinfo)
        all_users.append(uinfo)

    user_lst = storage_socket.users.list()
    user_lst_model = [UserInfo(**x) for x in user_lst]

    # Sort both lists by username
    all_users = sorted(all_users, key=lambda x: x.username)
    user_lst_model = sorted(user_lst_model, key=lambda x: x.username)

    d1 = [x.model_dump(exclude={"id"}) for x in all_users]
    d2 = [x.model_dump(exclude={"id"}) for x in user_lst_model]
    assert d1 == d2


def test_user_socket_delete(storage_socket: SQLAlchemySocket):
    uinfo1 = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    uinfo2 = UserInfo(
        username="bill",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo1)
    storage_socket.users.add(uinfo2)

    uid1 = storage_socket.users.get("george")["id"]
    uid2 = storage_socket.users.get("bill")["id"]

    # Raises exception on error
    storage_socket.users.delete("george")
    storage_socket.users.delete(uid2)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("george")
    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get(uid1)
    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("bill")
    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get(uid2)


def test_user_socket_use_unknown_user(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )
    storage_socket.users.add(uinfo)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.get("geoff")

    with pytest.raises(AuthenticationFailure, match=r"Incorrect username or password"):
        storage_socket.users.authenticate("geoff", "a password 1234")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        uinfo = UserInfo(id=1234, username="geoff", role="read", enabled=True)
        storage_socket.users.modify(uinfo, False)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.change_password("geoff", "a password 1234")

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.change_password("geoff", None)

    with pytest.raises(UserManagementError, match=r"User.*not found"):
        storage_socket.users.delete("geoff")


def test_user_socket_verify_password(storage_socket: SQLAlchemySocket):
    for idx, password in enumerate(["simple_password", "ABC 1234 abcd", "ÃØ©þꝎꟇÃØ©þꝎꟇ"]):
        username = f"george_{idx}"
        uinfo = UserInfo(
            username=username,
            role="read",
            enabled=True,
        )

        add_pw = storage_socket.users.add(uinfo, password=password)
        assert add_pw == password
        storage_socket.users.authenticate(username, add_pw)

        for guess in ["Simple_password", "ABC%1234 abcd", "ÃØ©þꝎBÃØ©þꝎꟇ"]:
            with pytest.raises(AuthenticationFailure):
                storage_socket.users.authenticate(username, guess)


def test_user_socket_verify_user_disabled(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    gen_pw = storage_socket.users.add(uinfo)

    uinfo2 = storage_socket.users.authenticate("george", gen_pw)

    uinfo2.enabled = False
    storage_socket.users.modify(uinfo2, as_admin=True)

    # A disabled account is revealed only to a caller who supplies the CORRECT password
    with pytest.raises(AuthenticationFailure, match=r"is disabled"):
        storage_socket.users.authenticate("george", gen_pw)

    # A wrong password against a disabled account stays generic, so the account cannot be
    # enumerated by someone who does not know the password
    with pytest.raises(AuthenticationFailure, match=r"^Incorrect username or password$"):
        storage_socket.users.authenticate("george", "the_wrong_password_1234")


def test_user_socket_change_password(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    old_pw = storage_socket.users.add(uinfo, "old_password_123")
    assert old_pw == "old_password_123"

    storage_socket.users.authenticate("george", "old_password_123")

    # update password...
    storage_socket.users.change_password("george", password="new_password_123")

    # Raises exception on failure
    storage_socket.users.authenticate("george", "new_password_123")

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("george", "old_password_123")


def test_user_socket_password_generation(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=True,
    )

    gen_pw = storage_socket.users.add(uinfo)
    storage_socket.users.authenticate("george", gen_pw)
    is_valid_password(gen_pw)
    storage_socket.users.authenticate("george", gen_pw)

    gen_pw_2 = storage_socket.users.change_password("george", None)
    storage_socket.users.authenticate("george", gen_pw_2)
    is_valid_password(gen_pw)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("george", gen_pw)


@pytest.mark.parametrize("as_admin", [True, False])
def test_user_socket_no_modify_username(storage_socket: SQLAlchemySocket, as_admin: bool):
    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=False,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo)
    uid = storage_socket.users.get("george")["id"]

    uinfo2 = UserInfo(
        id=uid, username="george2", role="admin", fullname="Test user 2", email="test@example.com", enabled=True
    )

    with pytest.raises(UserManagementError, match=r"Cannot change"):
        uinfo3 = storage_socket.users.modify(uinfo2, as_admin=as_admin)


@pytest.mark.parametrize("as_admin", [True, False])
def test_user_socket_modify(storage_socket: SQLAlchemySocket, as_admin: bool):
    # If as_admin == True for user.modify(), then all fields can be modified
    # Otherwise, some fields will always stay the same (enabled, role)

    uinfo = UserInfo(
        username="george",
        role="read",
        enabled=False,
        fullname="Test user",
        email="george@example.com",
        organization="My Org",
    )
    storage_socket.users.add(uinfo)
    uid = storage_socket.users.get("george")["id"]

    uinfo2 = UserInfo(
        id=uid, username="george", role="admin", fullname="Test user 2", email="test@example.com", enabled=True
    )

    # Modify should return the same this as get
    uinfo3 = storage_socket.users.modify(uinfo2, as_admin=as_admin)
    uinfo4 = storage_socket.users.get("george")

    assert uinfo3 == uinfo4

    if as_admin is True:
        assert uinfo2 == UserInfo(**uinfo3)
    else:
        # Stayed the same
        assert uinfo3["enabled"] == uinfo.enabled
        assert uinfo3["role"] == uinfo.role

        # Can be modified
        assert uinfo3["fullname"] == uinfo2.fullname
        assert uinfo3["email"] == uinfo2.email
        assert uinfo3["organization"] == uinfo2.organization


def test_user_socket_use_invalid_username(storage_socket: SQLAlchemySocket):
    for username in invalid_usernames:
        # Normally, UserInfo prevents bad usernames. But the socket also checks, as a last resort
        # So we have to bypass the UserInfo check with construct()
        uinfo = UserInfo.model_construct(
            username=username,
            role="read",
            enabled=True,
        )

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.add(uinfo, "password_1234")

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.get(username)

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.authenticate(username, "a_password_1234")

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.change_password(username, "a_password_1234")

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.change_password(username, None)

        with pytest.raises(InvalidUsernameError):
            storage_socket.users.delete(username)

    # Numeric usernames not allowed for some operations
    uinfo2 = UserInfo.model_construct(
        username="123456789",
        role="read",
        enabled=True,
    )

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.add(uinfo2, "password_1234")

    with pytest.raises(InvalidUsernameError):
        storage_socket.users.authenticate("123456789", "a_password_1234")


def test_user_socket_use_invalid_password(storage_socket: SQLAlchemySocket):
    for idx, password in enumerate(invalid_passwords):
        username = f"george_{idx}"

        uinfo = UserInfo.model_construct(
            username=username,
            role="read",
            enabled=True,
        )
        with pytest.raises(InvalidPasswordError):
            storage_socket.users.add(uinfo, password)

        #  Add for real now
        storage_socket.users.add(uinfo, "good_password")
        uid = storage_socket.users.get(username)["id"]

        with pytest.raises(InvalidPasswordError):
            storage_socket.users.change_password(username, password)

        with pytest.raises(InvalidPasswordError):
            storage_socket.users.change_password(uid, password)

        # At verification time, only structurally-invalid passwords are rejected outright.
        # Anything else is simply an incorrect password
        if password in invalid_verify_passwords:
            expected = InvalidPasswordError
        else:
            expected = AuthenticationFailure

        with pytest.raises(expected):
            storage_socket.users.authenticate(username, password)


def test_user_socket_new_password_length_policy(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(username="george", role="read", enabled=True)
    storage_socket.users.add(uinfo, "good_password")

    # 11 characters is too short, 12 is fine
    with pytest.raises(InvalidPasswordError, match="at least 12 characters"):
        storage_socket.users.change_password("george", "abcdefghijk")

    storage_socket.users.change_password("george", "abcdefghijkl")
    storage_socket.users.authenticate("george", "abcdefghijkl")

    # The maximum is in *bytes*, not characters. "é" is two bytes when encoded as UTF-8,
    # so 37 of them is 74 bytes and 36 of them is exactly 72
    with pytest.raises(InvalidPasswordError, match="at most 72 bytes"):
        storage_socket.users.change_password("george", "é" * 37)

    storage_socket.users.change_password("george", "é" * 36)
    storage_socket.users.authenticate("george", "é" * 36)

    # Same thing with plain ASCII (one byte per character)
    with pytest.raises(InvalidPasswordError, match="at most 72 bytes"):
        storage_socket.users.change_password("george", "a" * 73)

    storage_socket.users.change_password("george", "a" * 72)
    storage_socket.users.authenticate("george", "a" * 72)

    # Same policy applies when adding a user
    uinfo2 = UserInfo(username="bill", role="read", enabled=True)
    with pytest.raises(InvalidPasswordError, match="at least 12 characters"):
        storage_socket.users.add(uinfo2, "abcdefghijk")

    with pytest.raises(InvalidPasswordError, match="at most 72 bytes"):
        storage_socket.users.add(uinfo2, "é" * 37)

    storage_socket.users.add(uinfo2, "é" * 36)
    storage_socket.users.authenticate("bill", "é" * 36)


def test_user_socket_verify_legacy_short_password(storage_socket: SQLAlchemySocket):
    # A user created before the password policy was tightened must still be able to log in,
    # even though their password would not be accepted as a new password today
    legacy_pw = "simple"
    with pytest.raises(InvalidPasswordError):
        is_valid_password(legacy_pw)

    _seed_user_with_hash(storage_socket, "legacy_user", bcrypt.hashpw(legacy_pw.encode("UTF-8"), bcrypt.gensalt(6)))

    uinfo = storage_socket.users.authenticate("legacy_user", legacy_pw)
    assert uinfo.username == "legacy_user"

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("legacy_user", "Simple")


def test_user_socket_verify_legacy_long_password(storage_socket: SQLAlchemySocket):
    # bcrypt < 5 silently truncated at 72 bytes when hashing, so a stored hash for a longer
    # password was really computed from only the first 72 bytes. Logging in with the full
    # (long) password must still work
    legacy_pw = "x" * 100
    with pytest.raises(InvalidPasswordError):
        is_valid_password(legacy_pw)

    hashed = bcrypt.hashpw(legacy_pw.encode("UTF-8")[:72], bcrypt.gensalt(6))
    _seed_user_with_hash(storage_socket, "legacy_long_user", hashed)

    # Both the full password and its 72-byte prefix work, exactly as before
    storage_socket.users.authenticate("legacy_long_user", legacy_pw)
    storage_socket.users.authenticate("legacy_long_user", "x" * 72)

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("legacy_long_user", "y" * 100)


def test_bcrypt_cost_parsing():
    assert _bcrypt_cost(bcrypt.hashpw(b"abcdefghijkl", bcrypt.gensalt(6))) == 6
    assert _bcrypt_cost(bcrypt.hashpw(b"abcdefghijkl", bcrypt.gensalt(10))) == 10

    # All the bcrypt version prefixes we might find in an existing database
    assert _bcrypt_cost(b"$2a$04$" + b"x" * 53) == 4
    assert _bcrypt_cost(b"$2b$12$" + b"x" * 53) == 12
    assert _bcrypt_cost(b"$2y$08$" + b"x" * 53) == 8

    # Anything unparsable
    assert _bcrypt_cost(b"") is None
    assert _bcrypt_cost(b"not a hash at all") is None
    assert _bcrypt_cost(b"$1$abcdefg$") is None
    assert _bcrypt_cost(b"$2b$x1$" + b"x" * 53) is None
    assert _bcrypt_cost("$2b$12$" + "x" * 53) is None  # a str, not bytes


def test_user_socket_new_hashes_use_target_cost(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(username="george", role="read", enabled=True)
    storage_socket.users.add(uinfo, "good_password")

    assert _bcrypt_cost(_get_stored_hash(storage_socket, "george")) == BCRYPT_COST

    storage_socket.users.change_password("george", "another_good_password")
    assert _bcrypt_cost(_get_stored_hash(storage_socket, "george")) == BCRYPT_COST

    # Generated passwords too
    storage_socket.users.change_password("george", None)
    assert _bcrypt_cost(_get_stored_hash(storage_socket, "george")) == BCRYPT_COST


def test_user_socket_rehash_on_login(storage_socket: SQLAlchemySocket):
    password = "an_old_password"
    _seed_user_with_hash(storage_socket, "george", bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(6)))

    old_hash = _get_stored_hash(storage_socket, "george")
    assert _bcrypt_cost(old_hash) == 6

    storage_socket.users.authenticate("george", password)

    # Read back through a completely new session
    new_hash = _get_stored_hash(storage_socket, "george")
    assert new_hash != old_hash
    assert _bcrypt_cost(new_hash) == BCRYPT_COST

    # ... and the password still works
    storage_socket.users.authenticate("george", password)

    # Already upgraded - nothing changes on subsequent logins
    assert _get_stored_hash(storage_socket, "george") == new_hash


def test_user_socket_no_rehash_on_failed_login(storage_socket: SQLAlchemySocket):
    password = "an_old_password"
    _seed_user_with_hash(storage_socket, "george", bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(6)))

    old_hash = _get_stored_hash(storage_socket, "george")

    with pytest.raises(AuthenticationFailure):
        storage_socket.users.authenticate("george", "the_wrong_password")

    assert _get_stored_hash(storage_socket, "george") == old_hash


def test_user_socket_no_rehash_at_or_above_target_cost(storage_socket: SQLAlchemySocket):
    uinfo = UserInfo(username="george", role="read", enabled=True)
    storage_socket.users.add(uinfo, "good_password")

    # Hash is already at the target cost
    old_hash = _get_stored_hash(storage_socket, "george")
    assert _bcrypt_cost(old_hash) == BCRYPT_COST

    storage_socket.users.authenticate("george", "good_password")
    assert _get_stored_hash(storage_socket, "george") == old_hash

    # A stronger hash must never be downgraded
    password = "a_stronger_password"
    stronger_hash = bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(BCRYPT_COST + 1))
    _seed_user_with_hash(storage_socket, "bill", stronger_hash)

    storage_socket.users.authenticate("bill", password)
    assert _get_stored_hash(storage_socket, "bill") == stronger_hash


def test_user_socket_replace_password_hash_stale(storage_socket: SQLAlchemySocket):
    # Directly exercise the compare-and-set used to store an upgraded hash. This stands in for the
    # (hard to trigger deterministically) race where the password changes during a login
    uinfo = UserInfo(username="george", role="read", enabled=True)
    storage_socket.users.add(uinfo, "first_password")
    uid = storage_socket.users.get("george")["id"]

    stale_hash = _get_stored_hash(storage_socket, "george")

    storage_socket.users.change_password("george", "second_password")
    current_hash = _get_stored_hash(storage_socket, "george")
    assert current_hash != stale_hash

    # Compare-and-set against the old hash matches no rows. The password we verified against is
    # no longer valid, so authentication must fail...
    with pytest.raises(AuthenticationFailure, match="Incorrect username or password"):
        storage_socket.users._replace_password_hash(uid, stale_hash, _hash_password("first_password"), "first_password")

    # ... and the newer hash is left untouched
    assert _get_stored_hash(storage_socket, "george") == current_hash

    # If the concurrent change happened to set the same password, the login stands, but the
    # (stale) upgraded hash is still not written
    storage_socket.users.change_password("george", "second_password")
    newest_hash = _get_stored_hash(storage_socket, "george")
    assert newest_hash != current_hash

    storage_socket.users._replace_password_hash(uid, current_hash, _hash_password("second_password"), "second_password")
    assert _get_stored_hash(storage_socket, "george") == newest_hash


def test_user_socket_authenticate_indistinguishable_failures(storage_socket: SQLAlchemySocket):
    # An unknown user, a disabled user given a WRONG password, and a wrong password for an
    # enabled user must all look the same. A disabled user given the CORRECT password is told
    # the account is disabled (checked separately below and in test_user_socket_verify_user_disabled).
    password = "a_good_password"

    uinfo = UserInfo(username="george", role="read", enabled=True)
    storage_socket.users.add(uinfo, password)

    uinfo2 = UserInfo(username="bill", role="read", enabled=False)
    storage_socket.users.add(uinfo2, password)

    messages = []
    for username, guess in [("nobody", password), ("bill", "the_wrong_password"), ("george", "the_wrong_password")]:
        with pytest.raises(AuthenticationFailure) as excinfo:
            storage_socket.users.authenticate(username, guess)
        messages.append(str(excinfo.value))

    assert messages == ["Incorrect username or password"] * 3

    # The disabled account, given the correct password, is told that it is disabled
    with pytest.raises(AuthenticationFailure, match=r"is disabled"):
        storage_socket.users.authenticate("bill", password)


@pytest.mark.parametrize("password", [1234, None, ["a_password_1234"], b"a_password_1234", {}])
def test_user_socket_authenticate_nonstring_password(storage_socket: SQLAlchemySocket, password):
    uinfo = UserInfo(username="george", role="read", enabled=True)
    storage_socket.users.add(uinfo, "a_good_password")

    # Whatever arrived in the request body, this must be a clean error and not a TypeError
    with pytest.raises((AuthenticationFailure, InvalidPasswordError)):
        storage_socket.users.authenticate("george", password)

    # Same for a user that doesn't exist
    with pytest.raises((AuthenticationFailure, InvalidPasswordError)):
        storage_socket.users.authenticate("nobody", password)


def test_user_socket_authenticate_corrupt_stored_hash(storage_socket: SQLAlchemySocket):
    # A hash that bcrypt cannot make sense of must be a normal authentication failure,
    # not an internal error
    _seed_user_with_hash(storage_socket, "george", b"this is not a bcrypt hash")

    with pytest.raises(AuthenticationFailure, match=r"^Incorrect username or password$"):
        storage_socket.users.authenticate("george", "a_good_password")
