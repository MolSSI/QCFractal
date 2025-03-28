from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
    LargeBinary,
    Boolean,
    JSON,
    UniqueConstraint,
    Enum,
    select,
    TIMESTAMP,
)
from sqlalchemy.orm import relationship
from qcportal.utils import now_at_utc

from qcfractal.db_socket import BaseORM
from qcportal.auth import AuthTypeEnum

if TYPE_CHECKING:
    from typing import Optional, Iterable, Dict, Any


class UserGroupORM(BaseORM):
    """
    Table for storing which groups a user belongs to
    """

    __tablename__ = "user_groups"

    user_id = Column(Integer, ForeignKey("user.id", ondelete="cascade"), nullable=False, primary_key=True)
    group_id = Column(Integer, ForeignKey("group.id", ondelete="cascade"), nullable=False, primary_key=True)


class GroupORM(BaseORM):
    """
    Table for storing group information
    """

    __tablename__ = "group"

    id = Column(Integer, primary_key=True)
    groupname = Column(String, nullable=False)
    description = Column(String, nullable=False)

    __table_args__ = (UniqueConstraint("groupname", name="ux_group_groupname"),)


class RoleORM(BaseORM):
    """
    Table for storing role information
    """

    __tablename__ = "role"

    id = Column(Integer, primary_key=True)

    rolename = Column(String, nullable=False)
    permissions = Column(JSON, nullable=False)

    __table_args__ = (UniqueConstraint("rolename", name="ux_role_rolename"),)

    _qcportal_model_excludes = ["id"]


class UserORM(BaseORM):
    """
    Table for storing user information
    """

    __tablename__ = "user"

    id = Column(Integer, primary_key=True)

    role_id = Column(Integer, ForeignKey("role.id"), nullable=False)
    role_orm = relationship(RoleORM)

    groups_orm = relationship(GroupORM, secondary=UserGroupORM.__tablename__)

    auth_type = Column(Enum(AuthTypeEnum), nullable=False)

    username = Column(String, nullable=False)
    password = Column(LargeBinary, nullable=False)
    enabled = Column(Boolean, nullable=False, server_default="true")
    fullname = Column(String, nullable=False, server_default="")
    organization = Column(String, nullable=False, server_default="")
    email = Column(String, nullable=False, server_default="")

    __table_args__ = (UniqueConstraint("username", name="ux_user_username"),)

    _qcportal_model_excludes = ["role_orm", "groups_orm", "role_id", "password"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        d["role"] = self.role_orm.rolename
        d["groups"] = [x.groupname for x in self.groups_orm]
        return d


class UserSessionORM(BaseORM):
    """
    Table for storing flask session information
    """

    __tablename__ = "user_session"

    session_id = Column(String, nullable=False, primary_key=True)
    session_data = Column(LargeBinary, nullable=False)
    last_accessed = Column(TIMESTAMP(timezone=True), nullable=False, default=now_at_utc)


_user_id_map_subq = select(UserORM.id.label("id"), UserORM.username.label("username")).subquery()
_group_id_map_subq = select(GroupORM.id.label("id"), GroupORM.groupname.label("groupname")).subquery()


class UserIDMapSubquery(BaseORM):
    __table__ = _user_id_map_subq


class GroupIDMapSubquery(BaseORM):
    __table__ = _group_id_map_subq
