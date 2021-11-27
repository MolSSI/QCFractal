from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, ForeignKey, String, LargeBinary, Boolean, JSON, UniqueConstraint
from sqlalchemy.orm import relationship

from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Optional, Iterable, Dict, Any


class UserORM(BaseORM):

    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    role_id = Column(Integer, ForeignKey("role.id"), nullable=False)
    role_obj = relationship("RoleORM", lazy="select")  # or lazy='joined'

    username = Column(String, nullable=False)
    password = Column(LargeBinary, nullable=False)
    enabled = Column(Boolean, nullable=False, server_default="true")
    fullname = Column(String, nullable=False, server_default="")
    organization = Column(String, nullable=False, server_default="")
    email = Column(String, nullable=False, server_default="")

    __table_args__ = (UniqueConstraint("username", name="ux_user_username"),)

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Removes some sensitive or useless fields
        d = BaseORM.dict(self, exclude)
        d.pop("role_id", None)
        d.pop("password", None)
        d["role"] = self.role_obj.rolename
        return d


class RoleORM(BaseORM):

    __tablename__ = "role"

    id = Column(Integer, primary_key=True)

    rolename = Column(String, nullable=False)
    permissions = Column(JSON, nullable=False)

    __table_args__ = (UniqueConstraint("rolename", name="ux_role_rolename"),)
