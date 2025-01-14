from __future__ import annotations

from sqlalchemy import Column, Integer, String, Enum, TIMESTAMP, BigInteger
from sqlalchemy.dialects.postgresql import JSONB

from qcfractal.db_socket.base_orm import BaseORM
from qcportal.external_files import ExternalFileStatusEnum, ExternalFileTypeEnum
from qcportal.utils import now_at_utc


class ExternalFileORM(BaseORM):
    """
    Table for storing molecules
    """

    __tablename__ = "external_file"

    id = Column(Integer, primary_key=True)
    file_type = Column(Enum(ExternalFileTypeEnum), nullable=False)

    created_on = Column(TIMESTAMP, default=now_at_utc, nullable=False)
    status = Column(Enum(ExternalFileStatusEnum), nullable=False)

    file_name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    provenance = Column(JSONB, nullable=False)

    sha256sum = Column(String, nullable=False)
    file_size = Column(BigInteger, nullable=False)

    bucket = Column(String, nullable=False)
    object_key = Column(String, nullable=False)

    __mapper_args__ = {"polymorphic_on": "file_type"}

    _qcportal_model_excludes__ = ["object_key", "bucket"]
