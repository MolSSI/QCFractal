from __future__ import annotations

import logging
from sqlalchemy.exc import IntegrityError
from qcfractal.storage_sockets.models import RoleORM
from qcfractal.interface.models import RoleInfo
from qcfractal.storage_sockets.sqlalchemy_socket import AuthorizationFailure

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, List
    from sqlalchemy.orm.session import Session
    from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket

"""
Default roles are:
    * admin   (read/write to everything)
    * read    (read only, all resources except user, role, manager, access)
    * monitor (read only, all resources except user, role)
    * compute (for compute workers, read/write on queue_manager)
    * submit  (read on all except user, role, manager, access, write on task_queue, service_queue, molecule, keyword, collection)
"""

default_roles = {
    "admin": {
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
        ]
    },
    "read": {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Deny", "Action": "*", "Resource": ["user", "manager", "role", "access"]},
        ]
    },
    "monitor": {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Deny", "Action": "*", "Resource": ["user", "role"]},
        ]
    },
    "compute": {
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": ["queue_manager"]},
        ]
    },
    "submit": {
        "Statement": [
            {"Effect": "Allow", "Action": "GET", "Resource": "*"},
            {"Effect": "Deny", "Action": "*", "Resource": ["user", "manager", "role", "access"]},
            {
                "Effect": "Allow",
                "Action": "*",
                "Resource": ["task_queue", "service_queue", "molecule", "keyword", "collection"],
            },
        ]
    },
}


class RoleSocket:
    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _role_orm_to_model(role_orm: RoleORM) -> RoleInfo:
        return RoleInfo(**role_orm.dict(exclude={"id"}))

    def _get_internal(self, session: Session, rolename: str) -> RoleORM:
        """
        Returns a RoleORM for the given rolename, or raises an exception if it does not exist

        The returned ORM is attached to the session
        """

        rolename = rolename.lower()
        orm = session.query(RoleORM).filter(RoleORM.rolename == rolename).one_or_none()
        if orm is None:
            raise AuthorizationFailure(f"Role {rolename} does not exist")

        return orm

    def list(self) -> List[RoleInfo]:
        """
        Get information about all roles
        """
        with self._core_socket.session_scope() as session:
            roles = session.query(RoleORM).order_by(RoleORM.id.asc()).all()
            return [self._role_orm_to_model(x) for x in roles]

    def get(self, rolename: str) -> RoleInfo:
        """
        Get information about a particular role
        """
        with self._core_socket.session_scope() as session:
            role = self._get_internal(session, rolename)
            return self._role_orm_to_model(role)

    def add(self, rolename: str, permissions: Dict) -> None:
        """
        Adds a new role.

        Parameters
        ----------
        rolename : str
        permissions : Dict
            Examples:
                permissions = {
                "Statement": [
                    {"Effect": "Allow","Action": "*","Resource": "*"},
                    {"Effect": "Deny","Action": "GET","Resource": "user"},
                ]}
        """

        rolename = rolename.lower()
        with self._core_socket.session_scope() as session:
            try:
                role = RoleORM(rolename=rolename, permissions=permissions)  # type: ignore
                session.add(role)
            except IntegrityError as err:
                raise AuthorizationFailure(f"Role {rolename} already exists")

    def modify(self, rolename: str, permissions: Dict) -> None:
        """
        Update role's permissions.

        Parameters
        ----------
        rolename
            The name of the role to update
        permissions
            The new permissions to be associated with that role
        """

        # Cannot change admin role
        if rolename == "admin":
            raise AuthorizationFailure("Cannot modify the admin role")

        with self._core_socket.session_scope() as session:
            role = self._get_internal(session, rolename)
            role.permissions = permissions

    def delete(self, rolename: str):
        """
        Delete role.


        This will raise an exception if the role does not exist or is being referenced from somewhere else.

        Parameters
        ----------
        rolename
            The role name to delete
        """

        try:
            with self._core_socket.session_scope() as session:
                role = self._get_internal(session, rolename)
                session.delete(role)
        except IntegrityError:
            raise AuthorizationFailure("Role could not be deleted. Likely it is being referenced somewhere")
