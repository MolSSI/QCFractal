from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select

from qcportal.auth import RoleInfo, is_valid_rolename
from qcportal.exceptions import UserManagementError
from .db_models import RoleORM

if TYPE_CHECKING:
    from typing import Dict, List, Any, Optional
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket

"""
Default roles are:
    * admin   (read/write to everything)
    * read    (read only, all resources except user, role, manager, access, error)
    * monitor (read only, all resources except user, role)
    * compute (for compute workers, read/write on queue_manager)
    * submit  (read on all except user, role, manager, access, error
               write on task_queue, service_queue, molecule, keyword, collection)
"""

default_roles: Dict[str, Any] = {
    "admin": {
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
        ]
    },
    "read": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": ["/api/v1/users", "/api/v1/me"]},
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": [
                    "/api/v1/roles",
                    "/api/v1/managers",
                    "/api/v1/server_errors",
                    "/api/v1/access_logs",
                    "/api/v1/tasks",
                    "/api/v1/internal_jobs",
                ],
            },
        ]
    },
    "monitor": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": "/api/v1/users"},
            {"Effect": "Deny", "Action": "*", "Resource": ["/api/v1/roles"]},
        ]
    },
    "compute": {
        "Statement": [
            {"Effect": "Allow", "Action": ["READ"], "Resource": "/api/v1/information"},
            {"Effect": "Allow", "Action": ["READ"], "Resource": "/compute/v1/information"},
            {"Effect": "Allow", "Action": ["READ", "WRITE"], "Resource": "/api/v1/users"},
            {"Effect": "Allow", "Action": "*", "Resource": ["/compute/v1/managers", "/compute/v1/tasks"]},
        ]
    },
    "submit": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": "/api/v1/users"},
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": [
                    "/api/v1/roles",
                    "/api/v1/managers",
                    "/api/v1/server_errors",
                    "/api/v1/access_logs",
                    "/api/v1/tasks",
                    "/api/v1/internal_jobs",
                ],
            },
            {
                "Effect": "Allow",
                "Action": "*",
                "Resource": ["/api/v1/records", "/api/v1/molecules", "/api/v1/keywords", "/api/v1/datasets"],
            },
        ]
    },
}


class RoleSocket:
    """
    Socket for managing user roles
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

    def _get_internal(self, session: Session, rolename: str) -> RoleORM:
        """
        Returns a RoleORM for the given rolename

        If the user is not found, an exception is raised. The ORM is attached to the given session

        Parameters
        ----------
        session
            SQLAlchemy session to use for querying

        Returns
        -------
        :
            ORM of the specified role
        """

        is_valid_rolename(rolename)

        stmt = select(RoleORM).where(RoleORM.rolename == rolename)
        orm = session.execute(stmt).scalar_one_or_none()
        if orm is None:
            raise UserManagementError(f"Role {rolename} does not exist")

        return orm

    def list(self, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get information about all roles

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """
        with self.root_socket.optional_session(session, True) as session:
            stmt = select(RoleORM).order_by(RoleORM.id.asc())
            roles = session.execute(stmt).scalars().all()
            return [r.model_dict() for r in roles]

    def get(self, rolename: str, *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Get information about a particular role

        Parameters
        ----------
        rolename
            Name of the role
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """
        with self.root_socket.optional_session(session, True) as session:
            role = self._get_internal(session, rolename)
            return role.model_dict()

    def add(self, role_info: RoleInfo, *, session: Optional[Session] = None) -> None:
        """
        Adds a new role.

        An exception is raised if the role already exists.

        Parameters
        ----------
        role_info
            Data about the new role
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        is_valid_rolename(role_info.rolename)

        try:
            with self.root_socket.optional_session(session) as session:
                role = RoleORM(rolename=role_info.rolename, permissions=role_info.permissions.dict())
                session.add(role)
        except IntegrityError:
            raise UserManagementError(f"Role {role_info.rolename} already exists")

        self._logger.info(f"Role {role_info.rolename} added")

    def modify(self, role_info: RoleInfo, *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Update role's permissions.

        Parameters
        ----------
        role_info
            Data about the role (new permissions)
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        # Cannot change admin role
        if role_info.rolename == "admin":
            raise UserManagementError("Cannot modify the admin role")

        with self.root_socket.optional_session(session) as session:
            role = self._get_internal(session, role_info.rolename)
            role.permissions = role_info.permissions.dict()
            session.commit()

            self._logger.info(f"Role {role_info.rolename} modified")
            return self.get(role_info.rolename, session=session)

    def delete(self, rolename: str, *, session: Optional[Session] = None) -> None:
        """
        Delete role.

        This will raise an exception if the role does not exist or is being referenced from somewhere else.

        Parameters
        ----------
        rolename
            The role name to delete
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        # Cannot delete admin role
        if rolename == "admin":
            raise UserManagementError("Cannot delete the admin role")

        try:
            with self.root_socket.optional_session(session) as session:
                role = self._get_internal(session, rolename)
                session.delete(role)
        except IntegrityError:
            raise UserManagementError("Role could not be deleted. Likely it is being referenced somewhere")

        self._logger.info(f"Role {rolename} deleted")

    def reset_defaults(self, *, session: Optional[Session] = None) -> None:
        """
        Reset the permissions of the default roles back to their original values

        If a role does not exist, it will be created. Manually-created roles will be left alone.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session) as session:
            for rolename, permissions in default_roles.items():
                stmt = select(RoleORM).where(RoleORM.rolename == rolename)
                role_data = session.execute(stmt).scalar_one_or_none()
                if role_data is None:
                    role_data = RoleORM(rolename=rolename, permissions=permissions)
                    session.add(role_data)
                else:
                    role_data.permissions = permissions

        self._logger.warning(f"Reset all roles to defaults")
