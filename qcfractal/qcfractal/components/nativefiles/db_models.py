from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, String, Integer, Enum, LargeBinary, ForeignKey, Index, UniqueConstraint, Boolean

from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class NativeFileORM(BaseORM):
    """
    Table for storing raw, program-dependent raw data
    """

    __tablename__ = "native_file"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)

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

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "record_id", "id")
        return BaseORM.model_dict(self, exclude)
