from __future__ import annotations

import logging
import secrets
from typing import TYPE_CHECKING

import bcrypt
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select

from qcportal.auth import UserInfo, is_valid_password, is_valid_username, AuthTypeEnum
from qcportal.exceptions import AuthenticationFailure, UserManagementError, InvalidRolenameError
from .db_models import UserORM, UserGroupORM, UserPreferencesORM
from .role_permissions import GLOBAL_ROLE_PERMISSIONS

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, List, Dict, Any, Union, Tuple, Set


valid_roles: Set[str] = set(GLOBAL_ROLE_PERMISSIONS.keys())


def is_valid_role(role: str):
    if role not in valid_roles:
        raise InvalidRolenameError(f"Role with name '{role} does not exist")


def _generate_password() -> str:
    """
    Generates a random password

    Returns
    -------
    :
        An plain-text random password.
    """
    return secrets.token_urlsafe(16)


def _hash_password(password: str) -> bytes:
    """
    Hashes a password in a consistent way
    """

    return bcrypt.hashpw(password.encode("UTF-8"), bcrypt.gensalt(6))


class UserSocket:
    """
    Socket for managing users
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

    def _get_internal(self, session: Session, username_or_id: Union[int, str]) -> UserORM:
        """
        Obtain the ORM for a particular user.

        If the user is not found, an exception is raised. The ORM is attached to the given session

        Parameters
        ----------
        session
            SQLAlchemy session to use for querying
        username_or_id
            The username or user ID

        Returns
        -------
        :
            ORM of the specified user
        """

        if isinstance(username_or_id, int) or username_or_id.isdecimal():
            stmt = select(UserORM).where(UserORM.id == username_or_id)
        else:
            is_valid_username(username_or_id)
            stmt = select(UserORM).where(UserORM.username == username_or_id)

        user = session.execute(stmt).scalar_one_or_none()

        if user is None:
            raise UserManagementError(f"User {username_or_id} not found.")

        return user

    def _assert_user_exists(self, session: Session, username_or_id: Union[int, str]) -> None:
        # Just call the existing function, swallowing the return
        _ = self._get_internal(session, username_or_id)

    def list(self, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get information about all users

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(UserORM).order_by(UserORM.id.asc())
            all_users = session.execute(stmt).scalars().all()
            return [x.model_dict() for x in all_users]

    def get(self, username_or_id: Union[int, str], *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Obtains information for a user

        Returns all info for a user, except (hashed) password

        Parameters
        ----------
        username_or_id
            The username or user ID
        session
            An existing SQLAlchemy session to use. If None, one will be created
        """

        with self.root_socket.optional_session(session, True) as session:
            user = self._get_internal(session, username_or_id)
            return user.model_dict()

    def add(self, user_info: UserInfo, password: Optional[str] = None, *, session: Optional[Session] = None) -> str:
        """
        Adds a new user

        Parameters
        ----------
        user_info
            New user's information
        password
            The user's password. If None, a new password will be generated.
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            The password for the user. This is useful if the password is autogenerated
        """

        # Should have been checked already, but defense in depth
        is_valid_username(user_info.username)
        is_valid_role(user_info.role)

        # ID should not be set
        if user_info.id is not None:
            raise UserManagementError("Cannot add a user - id was given as part of new user info")

        if password is None:
            password = _generate_password()

        is_valid_password(password)

        hashed_pw = _hash_password(password)

        # Groups are not directly a part of the ORM
        user_dict = user_info.dict(exclude={"groups"})

        try:
            with self.root_socket.optional_session(session) as session:
                # Will raise exception if group does not exist or name is invalid
                groups = [self.root_socket.groups._get_internal(session, g) for g in user_info.groups]

                user = UserORM(**user_dict, groups_orm=groups, password=hashed_pw)
                session.add(user)
        except IntegrityError:
            raise UserManagementError(f"User {user_info.username} already exists")

        self._logger.info(f"User {user_info.username} added")
        return password

    def _verify_local_password(self, user: UserORM, password: str):
        """
        Verifies a given username and password against the local db

        Raises exception if the password does not match or there is another problem
        """

        is_valid_password(password)

        try:
            pwcheck = bcrypt.checkpw(password.encode("UTF-8"), user.password)
        except Exception as e:
            self._logger.error(f"Password check failure for user {user.username}, error: {str(e)}")
            self._logger.error(
                f"Error likely caused by encryption salt mismatch, potentially fixed by creating a new password for user {user.username}."
            )
            raise UserManagementError("Password decryption failure, please contact your system administrator.")

        if pwcheck is False:
            raise AuthenticationFailure("Incorrect username or password")

    def authenticate(self, username: str, password: str, *, session: Optional[Session] = None) -> UserInfo:
        """
        Authenticates a given username and password, returning all info about the user

        If the user is not found, or is disabled, or the password is incorrect, an exception is raised.

        Parameters
        ----------
        username
            The username of the user
        password
            The password associated with the username
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        --------
        :
            All information about the user
        """

        is_valid_username(username)

        with self.root_socket.optional_session(session, True) as session:
            try:
                user = self._get_internal(session, username)
            except UserManagementError as e:
                # Turn missing user into an Authentication error
                raise AuthenticationFailure("Incorrect username or password")

            if not user.enabled:
                raise AuthenticationFailure(f"User {username} is disabled.")

            # what's next depends on how the user is authenticated
            if user.auth_type == AuthTypeEnum.password:
                self._verify_local_password(user=user, password=password)
            else:
                self._logger.error(f"Unknown auth type: {user.auth_type}. This is a developer error")
                raise UserManagementError(f"Unknown authentication type stored in the database: {user.auth_type}")

            return user.to_model(UserInfo)

    def modify(self, user_info: UserInfo, as_admin: bool, *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Alters a user's information

        The user to modify is taken from the user_info object.

        The user's username or password cannot be changed this way. If `as_admin` is False, then only
        the descriptive changes (email, etc) can be changed. If it is True, then
        the `enabled` and `role` fields can also be changed.

        Parameters
        ----------
        user_info
            The user info to update the database with
        as_admin
            Enable changing sensitive columns (enabled & role)
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            An updated version of the user info, with all possible/allowed changes

        """

        with self.root_socket.optional_session(session) as session:
            user = self._get_internal(session, user_info.id)

            if user_info.username != user.username:
                raise UserManagementError("Cannot change username")

            user.fullname = user_info.fullname
            user.organization = user_info.organization
            user.email = user_info.email

            if as_admin is True:
                is_valid_role(user_info.role)
                groups = [self.root_socket.groups._get_internal(session, g) for g in user_info.groups]

                user.enabled = user_info.enabled
                user.role = user_info.role
                user.groups_orm = groups

            session.commit()

            self._logger.info(f"User {user_info.username} modified")

            return self.get(user_info.username, session=session)

    def change_password(
        self, username_or_id: Union[int, str], password: Optional[str], *, session: Optional[Session] = None
    ) -> str:
        """
        Alters a user's password

        Parameters
        ----------
        username_or_id
            The username or ID of the user
        password
            The user's new password. If the password is empty, an exception is raised. If None, then a
            password will be generated
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        -------
        :
            A string representing the password. If a new password was given, this should be identical
            to the input password. Otherwise, it will be the generated password.
        """

        if password is None:
            password = _generate_password()

        is_valid_password(password)

        with self.root_socket.optional_session(session) as session:
            user = self._get_internal(session, username_or_id)
            user.password = _hash_password(password)

        self._logger.info(f"Password for {username_or_id} modified")
        return password

    def delete(self, username_or_id: Union[int, str], *, session: Optional[Session] = None) -> None:
        """Removes a user

        This will raise an exception if the user doesn't exist or is being referenced elsewhere in the
        database.

        Parameters
        ----------
        username_or_id
            The username or ID of the user
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        try:
            with self.root_socket.optional_session(session) as session:
                user = self._get_internal(session, username_or_id)
                session.delete(user)
        except IntegrityError:
            raise UserManagementError("User could not be deleted. Likely it is being referenced somewhere")

        self._logger.info(f"User {username_or_id} deleted")

    def get_optional_user_id(
        self,
        username_or_id: Optional[Union[int, str]],
        *,
        session: Optional[Session] = None,
    ) -> Optional[int]:
        """
        Obtain the ID of a user

        If username_or_id is None, None is returned.

        If an ID or name that does not exist is given, an exception is raised.
        """

        if username_or_id is None:
            return None

        with self.root_socket.optional_session(session) as session:
            user = self._get_internal(session, username_or_id)
            return user.id

    def get_preferences(self, user_id: int, *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Get the user-set preferences for a given user

        Raises an exception if the user is not found
        """

        stmt = select(UserPreferencesORM).where(UserPreferencesORM.user_id == user_id)

        with self.root_socket.optional_session(session, True) as session:
            self._assert_user_exists(session, user_id)

            r = session.execute(stmt).scalar_one_or_none()
            if r is None:
                return {}

            return r.preferences

    def set_preferences(self, user_id: int, preferences: Dict[str, Any], *, session: Optional[Session] = None) -> None:
        """
        Sets a users preferences for a given user
        """

        with self.root_socket.optional_session(session) as session:
            self._assert_user_exists(session, user_id)

            stmt = select(UserPreferencesORM).where(UserPreferencesORM.user_id == user_id)
            prefs_orm = session.execute(stmt).scalar_one_or_none()
            if prefs_orm is None:
                prefs_orm = UserPreferencesORM(user_id=user_id, preferences=preferences)
                session.add(prefs_orm)
            else:
                prefs_orm.preferences = preferences
