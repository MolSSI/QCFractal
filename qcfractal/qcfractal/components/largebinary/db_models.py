from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, String, Enum, LargeBinary, BigInteger, ForeignKey

from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum
from qcfractal.components.record_db_models import BaseRecordORM

if TYPE_CHECKING:
    pass


class LargeBinaryORM(BaseORM):
    """
    Table for storing large amounts of binary data
    """

    __tablename__ = "largebinary_store"

    id = Column(Integer, primary_key=True)

    record_id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), nullable=False)
    size = Column(BigInteger, nullable=False)
    checksum = Column(String, nullable=False)
    compression_type = Column(Enum(CompressionEnum), nullable=False)

    # This column is marked as STORAGE EXTERNAL, which disables compression
    # (this is done elsewhere, I don't know of a way to do it declaratively)
    data_local = Column(LargeBinary, nullable=False)
