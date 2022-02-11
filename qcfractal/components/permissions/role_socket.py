from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select

from qcportal.exceptions import UserManagementError
from qcportal.permissions import RoleInfo, is_valid_rolename
from .db_models import RoleORM

if TYPE_CHECKING:
    from typing import Dict, List, Any, Optional
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket

    RoleInfoDict = Dict[str, Any]

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
            {"Effect": "Allow", "Action": "WRITE", "Resource": "me"},
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": ["users", "roles", "managers", "server_errors", "access_logs", "tasks"],
            },
        ]
    },
    "monitor": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": "me"},
            {"Effect": "Deny", "Action": "*", "Resource": ["users", "roles"]},
        ]
    },
    "compute": {
        "Statement": [
            {"Effect": "Allow", "Action": ["READ"], "Resource": "information"},
            {"Effect": "Allow", "Action": ["READ", "WRITE"], "Resource": "me"},
            {"Effect": "Allow", "Action": "*", "Resource": ["tasks", "managers"]},
        ]
    },
    "submit": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": "me"},
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": ["users", "roles", "managers", "server_errors", "access_logs", "tasks"],
            },
            {
                "Effect": "Allow",
                "Action": "*",
                "Resource": ["records", "molecules", "keywords", "datasets"],
            },
        ]
    },
}


class RoleSocket:
    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

    def _get_internal(self, session: Session, rolename: str) -> RoleORM:
        """
        Returns a RoleORM for the given rolename, or raises an exception if it does not exist

        The returned ORM is attached to the session
        """

        is_valid_rolename(rolename)

        stmt = select(RoleORM).where(RoleORM.rolename == rolename)
        orm = session.execute(stmt).scalar_one_or_none()
        if orm is None:
            raise UserManagementError(f"Role {rolename} does not exist")

        return orm

    def list(self, *, session: Optional[Session] = None) -> List[RoleInfoDict]:
        """
        Get information about all roles

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created
        """
        with self.root_socket.optional_session(session, True) as session:
            stmt = select(RoleORM).order_by(RoleORM.id.asc())
            roles = session.execute(stmt).scalars().all()
            return [r.dict() for r in roles]

    def get(self, rolename: str, *, session: Optional[Session] = None) -> RoleInfoDict:
        """
        Get information about a particular role

        Parameters
        ----------
        rolename
            Name of the role
        session
            An existing SQLAlchemy session to use. If None, one will be created
        """
        with self.root_socket.optional_session(session, True) as session:
            role = self._get_internal(session, rolename)
            return role.dict()

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
            is used, it will be flushed before returning from this function.
        """

        is_valid_rolename(role_info.rolename)

        try:
            with self.root_socket.optional_session(session) as session:
                role = RoleORM(rolename=role_info.rolename, permissions=role_info.permissions.dict())
                session.add(role)
        except IntegrityError:
            raise UserManagementError(f"Role {role_info.rolename} already exists")

        self._logger.info(f"Role {role_info.rolename} added")

    def modify(self, role_info: RoleInfo, *, session: Optional[Session] = None) -> RoleInfoDict:
        """
        Update role's permissions.

        Parameters
        ----------
        role_info
            Data about the role (new permissions)
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.
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
            is used, it will be flushed before returning from this function.
        """

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
