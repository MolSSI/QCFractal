from __future__ import annotations

from sqlalchemy import Column, String, Integer, Enum, LargeBinary, ForeignKey, Index, UniqueConstraint, Boolean

from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum


class NativeFileORM(BaseORM):
    __tablename__ = "native_file"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="CASCADE"), nullable=False)

    name = Column(String, nullable=False)
    compression = Column(Enum(CompressionEnum), nullable=False)
    compression_level = Column(Integer, nullable=False)
    is_text = Column(Boolean, nullable=False)
    uncompressed_size = Column(Integer, nullable=False)
    data = Column(LargeBinary, nullable=False)

    __table_args__ = (
        Index("ix_native_file_record_id", "record_id"),
        UniqueConstraint("record_id", "name", name="ux_native_file_record_id_name"),
    )
