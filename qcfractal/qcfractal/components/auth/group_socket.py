from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select

from qcportal.auth import is_valid_groupname, GroupInfo
from qcportal.exceptions import UserManagementError
from .db_models import GroupORM

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, List, Dict, Any, Union


class GroupSocket:
    """
    Socket for managing groups
    """

    def __init__(self, root_socket: SQLAlchemySocket):
        self.root_socket = root_socket
        self._logger = logging.getLogger(__name__)

    def _get_internal(self, session: Session, groupname_or_id: Union[int, str]) -> GroupORM:
        """
        Obtain the ORM for a particular group.

        If the group is not found, an exception is raised. The ORM is attached to the given session

        Parameters
        ----------
        session
            SQLAlchemy session to use for querying

        Returns
        -------
        :
            ORM of the specified group
        """

        if isinstance(groupname_or_id, int) or groupname_or_id.isnumeric():
            stmt = select(GroupORM).where(GroupORM.id == groupname_or_id)
        else:
            is_valid_groupname(groupname_or_id)
            stmt = select(GroupORM).where(GroupORM.groupname == groupname_or_id)

        group = session.execute(stmt).scalar_one_or_none()

        if group is None:
            raise UserManagementError(f"Group {groupname_or_id} does not exist.")

        return group

    def list(self, *, session: Optional[Session] = None) -> List[Dict[str, Any]]:
        """
        Get information about all groups

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        with self.root_socket.optional_session(session, True) as session:
            stmt = select(GroupORM).order_by(GroupORM.id.asc())
            all_groups = session.execute(stmt).scalars().all()
            return [x.model_dict() for x in all_groups]

    def get(self, groupname_or_id: Union[int, str], *, session: Optional[Session] = None) -> Dict[str, Any]:
        """
        Obtains information for a group

        Parameters
        ----------
        groupname_or_id
            The name or ID of the group
        session
            An existing SQLAlchemy session to use. If None, one will be created
        """

        with self.root_socket.optional_session(session, True) as session:
            group = self._get_internal(session, groupname_or_id)
            return group.model_dict()

    def add(self, group_info: GroupInfo, *, session: Optional[Session] = None):
        """
        Adds a new group

        Parameters
        ----------
        group_info
            Information about the new group
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        # Should have been checked already, but defense in depth
        is_valid_groupname(group_info.groupname)

        # ID should not be set
        if group_info.id is not None:
            raise UserManagementError("Cannot add a group - id was given as part of new group info")

        try:
            with self.root_socket.optional_session(session) as session:
                group = GroupORM(groupname=group_info.groupname, description=group_info.description)
                session.add(group)
        except IntegrityError:
            raise UserManagementError(f"Group {group_info.groupname} already exists")

        self._logger.info(f"Group {group_info.groupname} added")

    def delete(self, groupname_or_id: Union[int, str], *, session: Optional[Session] = None) -> None:
        """Removes a group

        This will raise an exception if the group doesn't exist or is being referenced elsewhere in the
        database.

        Parameters
        ----------
        groupname_or_id
            The name or ID of the group
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed (but not committed) before returning from this function.
        """

        try:
            with self.root_socket.optional_session(session) as session:
                group = self._get_internal(session, groupname_or_id)
                session.delete(group)
        except IntegrityError:
            raise UserManagementError("Group could not be deleted. Likely it is being referenced somewhere")

        self._logger.info(f"Group {groupname_or_id} deleted")
