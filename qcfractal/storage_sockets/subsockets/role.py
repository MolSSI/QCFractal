from __future__ import annotations

import secrets
import bcrypt
from sqlalchemy.exc import IntegrityError
from qcfractal.storage_sockets.models import UserORM, RoleORM
from qcfractal.storage_sockets.storage_utils import add_metadata_template, get_metadata_template
from qcfractal.storage_sockets.sqlalchemy_socket import get_count_fast

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Dict, Optional, Tuple, Any

class RoleSocket:
    def __init__(self, core_socket):
        self._core_socket = core_socket

    def list(self):
        """
        get all roles
        """
        with self._core_socket.session_scope() as session:
            data = session.query(RoleORM).filter().all()
            data = [x.to_dict(exclude=["id"]) for x in data]
        return data

    def get(self, rolename: str):
        """"""
        if rolename is None:
            return False, f"Role {rolename} not found."

        rolename = rolename.lower()
        with self._core_socket.session_scope() as session:
            data = session.query(RoleORM).filter_by(rolename=rolename).first()

            if data is None:
                return False, f"Role {rolename} not found."
            role = data.to_dict(exclude=["id"])
        return True, role

    def add(self, rolename: str, permissions: Dict):
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


        Returns
        -------
        bool :
            A Boolean of success flag
        """

        rolename = rolename.lower()
        with self._core_socket.session_scope() as session:
            blob = {"rolename": rolename, "permissions": permissions}

            try:
                role = RoleORM(**blob)
                session.add(role)
                return True, f"Role: {rolename} was added successfully."
            except IntegrityError as err:
                self._core_socket.logger.warning(str(err))
                session.rollback()
                return False, str(err.orig.args)

    def add_default(self):
        """
        Add default roles to the DB IF they don't exists

        Default roles are Admin, read (readonly)

        """

        read_permissions = {
            "Statement": [
                {"Effect": "Allow", "Action": "GET", "Resource": "*"},
                {"Effect": "Deny", "Action": "*", "Resource": ["user", "manager", "role"]},
            ]
        }

        admin_permissions = {
            "Statement": [
                {"Effect": "Allow", "Action": "*", "Resource": "*"},
            ]
        }

        with self._core_socket.session_scope() as session:
            user1 = {"rolename": "read", "permissions": read_permissions}
            user2 = {"rolename": "admin", "permissions": admin_permissions}

            try:
                session.add_all([RoleORM(**user1), RoleORM(**user2)])
                session.commit()
                return True
            except Exception:
                session.rollback()
                return False

    def update(self, rolename: str, permissions: Dict):
        """
        Update role's permissions.

        Parameters
        ----------
        rolename : str
        permissions : Dict

        Returns
        -------
        bool :
            A Boolean of success flag
        """

        rolename = rolename.lower()
        with self._core_socket.session_scope() as session:
            role = session.query(RoleORM).filter_by(rolename=rolename).first()

            if role is None:
                return False, f"Role {rolename} is not found."

            success = session.query(RoleORM).filter_by(rolename=rolename).update({"permissions": permissions})

        return success

    def delete(self, rolename: str):
        """
        Delete role.

        Parameters
        ----------
        rolename : str

        Returns
        -------
        bool :
            A Boolean of success flag
        """
        with self._core_socket.session_scope() as session:
            success = session.query(RoleORM).filter_by(rolename=rolename.lower()).delete()

        return success


