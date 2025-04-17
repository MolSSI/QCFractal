from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    String,
    LargeBinary,
    Boolean,
    UniqueConstraint,
    Index,
    Enum,
    select,
    TIMESTAMP,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from qcfractal.db_socket import BaseORM
from qcportal.auth import AuthTypeEnum
from qcportal.utils import now_at_utc

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


class UserORM(BaseORM):
    """
    Table for storing user information
    """

    __tablename__ = "user"

    id = Column(Integer, primary_key=True)

    role = Column(String, nullable=False)

    groups_orm = relationship(GroupORM, secondary=UserGroupORM.__tablename__)

    auth_type = Column(Enum(AuthTypeEnum), nullable=False)

    username = Column(String, nullable=False)
    password = Column(LargeBinary, nullable=False)
    enabled = Column(Boolean, nullable=False, server_default="true")
    fullname = Column(String, nullable=False, server_default="")
    organization = Column(String, nullable=False, server_default="")
    email = Column(String, nullable=False, server_default="")

    __table_args__ = (UniqueConstraint("username", name="ux_user_username"),)

    _qcportal_model_excludes = ["groups_orm", "password"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        d["groups"] = [x.groupname for x in self.groups_orm]
        return d


class UserSessionORM(BaseORM):
    """
    Table for storing flask session information
    """

    __tablename__ = "user_session"

    public_id = Column(Integer, primary_key=True)
    session_key = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("user.id", ondelete="cascade"), nullable=False)
    session_data = Column(JSONB, nullable=False)
    last_accessed = Column(TIMESTAMP(timezone=True), nullable=False, default=now_at_utc)

    __table_args__ = (
        UniqueConstraint("session_key", name="ux_user_session_session_key"),
        Index("ix_user_session_user_id", "user_id"),
    )

    def public_dict(self) -> Dict[str, Any]:
        return {
            "public_id": self.public_id,
            "user_id": self.user_id,
            "last_accessed": self.last_accessed,
            "ip_address": self.session_data.get("ip_address", None),
            "user_agent": self.session_data.get("user_agent", None),
        }


class UserPreferencesORM(BaseORM):
    """
    Table for storing user preference information
    """

    __tablename__ = "user_preferences"

    user_id = Column(Integer, ForeignKey("user.id", ondelete="cascade"), primary_key=True)
    preferences = Column(JSONB, nullable=False)


_user_id_map_subq = select(UserORM.id.label("id"), UserORM.username.label("username")).subquery()


class UserIDMapSubquery(BaseORM):
    __table__ = _user_id_map_subq
