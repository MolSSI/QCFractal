from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Tuple, List, Dict, Any, Optional

from qcportal.auth import UserInfo, RoleInfo
from qcportal.exceptions import AuthorizationFailure
from .policyuniverse import Policy

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket


class AuthSocket:
    """
    Socket for authenticating and authorizing
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

        self.security_enabled = self.root_socket.qcf_config.enable_security
        self.allow_unauthenticated_read = self.root_socket.qcf_config.allow_unauthenticated_read

        self.unauth_read_permissions = self.root_socket.roles.get("read")["permissions"]
        self.protected_resources = {"users", "roles", "me"}

    def authenticate(
        self, username: str, password: str, *, session: Optional[Session] = None
    ) -> Tuple[UserInfo, RoleInfo]:
        """
        Authenticates a given username and password, returning info about the user and their role

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
            All information about the user, and all information about the user's role
        """

        with self.root_socket.optional_session(session, True) as session:
            user_info = self.root_socket.users.authenticate(username=username, password=password, session=session)
            role_info_dict = self.root_socket.roles.get(user_info.role, session=session)
            return user_info, RoleInfo(**role_info_dict)

    def verify(self, user_id: int, *, session: Optional[Session] = None) -> Tuple[UserInfo, RoleInfo]:
        """
        Verifies that a given user id exists and is enabled, returning info about the user and their role

        This does not check the user's password.

        If the user is not found, or is disabled, an exception is raised.

        Parameters
        ----------
        user_id
            The id of the user to check
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.

        Returns
        --------
        :
            All information about the user, and all information about the user's role
        """

        with self.root_socket.optional_session(session, True) as session:
            user_info = self.root_socket.users.verify(user_id=user_id, session=session)
            role_info_dict = self.root_socket.roles.get(user_info.role, session=session)
            return user_info, RoleInfo(**role_info_dict)

    def is_authorized(
        self, resource: Dict[str, Any], action: str, subject: Dict[str, Any], context: Dict[str, Any], policies: Any
    ) -> Tuple[bool, str]:
        """
        Check for access to the given resource given permissions

        1. If no security (enable_security is False), always allow
        2. Check if allowed by the stored policies
        3. If denied, and allow_unauthenticated_read==True, use the default read permissions.

        Parameters
        ----------
        resource
            Dictionary describing the resource being accessed
        action
            The action being requested on the resource
        subject
            Dictionary describing the entity wanting access to the resource
        context
            Additional context of the request

        Returns
        -------
        :
            True if the access is allowed, False otherwise. The second element of the tuple
            is a string describing why the access was disallowed (if the first element is False)
        """

        # if no auth required, always allowed, except for protected resources
        if self.security_enabled is False:
            if resource["type"] in self.protected_resources:
                return False, f"Cannot access {resource} with security disabled"
            else:
                return True, "Allowed"

        if subject["username"] is None and not self.allow_unauthenticated_read:
            return False, "Server requires login"

        # uppercase by convention
        action = action.upper()

        context = {"Principal": subject["username"], "Action": action, "Resource": resource["type"]}

        policy = Policy(policies)
        if not policy.evaluate(context):
            # If that doesn't work, but we allow unauthenticated read, then try that
            if not self.allow_unauthenticated_read:
                return False, f"User {subject} is not authorized to access '{resource}'"

            if not Policy(self.unauth_read_permissions).evaluate(context):
                return False, f"User {subject} is not authorized to access '{resource}'"

        return True, "Allowed"

    def assert_authorized(
        self, resource: Dict[str, Any], action: str, subject: Dict[str, Any], context: Dict[str, Any], policies: Any
    ) -> None:
        """
        Check for access to the given resource given permissions

        1. If no security (enable_security is False), always allow
        2. Check if allowed by the stored policies
        3. If denied, and allow_unauthenticated_read==True, use the default read permissions.

        If authorization fails, an exception is raised

        Parameters
        ----------
        resource
            Dictionary describing the resource being accessed
        action
            The action being requested on the resource
        subject
            Dictionary describing the entity wanting access to the resource
        context
            Additional context of the request
        """

        allowed, msg = self.is_authorized(
            resource=resource, action=action, subject=subject, context=context, policies=policies
        )
        if not allowed:
            raise AuthorizationFailure(msg)

    def allowed_actions(self, subject: Any, resources: Any, actions: Any, policies: Any) -> List[Tuple[str, str]]:
        allowed: List[Tuple[str, str]] = []

        # if no auth required, always allowed, except for protected endpoints
        if self.security_enabled is False:
            for resource in resources:
                endpoint_last = resource.split("/")[-1]
                if endpoint_last in self.protected_resources:
                    continue
                allowed.extend((resource, x) for x in actions)
        else:
            read_policy = Policy(self.unauth_read_permissions)
            policy = Policy(policies)

            for resource in resources:
                for action in actions:
                    context = {"Principal": subject["username"], "Action": action, "Resource": resource}
                    if policy.evaluate(context):
                        allowed.append((resource, action))
                    elif (
                        self.allow_unauthenticated_read
                        and read_policy.evaluate(context)
                        and not resource.endswith("/me")
                    ):
                        allowed.append((resource, action))

        return allowed
