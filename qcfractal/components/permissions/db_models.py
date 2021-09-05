from sqlalchemy import Column, Integer, ForeignKey, String, LargeBinary, Boolean, JSON
from sqlalchemy.orm import relationship

from qcfractal.db_socket import BaseORM


class UserORM(BaseORM):

    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    role_id = Column(Integer, ForeignKey("role.id"), nullable=False)
    role_obj = relationship("RoleORM", lazy="select")  # or lazy='joined'

    username = Column(String, nullable=False, index=True, unique=True)  # indexed and unique
    password = Column(LargeBinary, nullable=False)
    enabled = Column(Boolean, nullable=False, server_default="true")
    fullname = Column(String, nullable=False, server_default="")
    organization = Column(String, nullable=False, server_default="")
    email = Column(String, nullable=False, server_default="")


class RoleORM(BaseORM):

    __tablename__ = "role"

    id = Column(Integer, primary_key=True)

    rolename = Column(String, nullable=False, unique=True)  # indexed and unique
    permissions = Column(JSON, nullable=False)
