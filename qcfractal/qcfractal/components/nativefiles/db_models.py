from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, String, Integer, Enum, LargeBinary, ForeignKey, UniqueConstraint, event, DDL
from sqlalchemy.orm import deferred

from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum, decompress

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
    compression_type = Column(Enum(CompressionEnum), nullable=False)
    compression_level = Column(Integer, nullable=False)
    data = deferred(Column(LargeBinary, nullable=False))

    __table_args__ = (UniqueConstraint("record_id", "name", name="ux_native_file_record_id_name"),)

    def get_file(self) -> Any:
        return decompress(self.data, self.compression_type)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "record_id", "compression_level")
        return BaseORM.model_dict(self, exclude)


# Mark the storage of the data_local column as external
event.listen(
    NativeFileORM.__table__,
    "after_create",
    DDL("ALTER TABLE native_file ALTER COLUMN data SET STORAGE EXTERNAL").execute_if(dialect=("postgresql")),
)
