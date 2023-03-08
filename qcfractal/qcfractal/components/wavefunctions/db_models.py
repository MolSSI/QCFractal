from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint, DDL, event, LargeBinary, Enum
from sqlalchemy.orm import deferred

from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum, decompress
from qcportal.wavefunctions import WavefunctionProperties

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class WavefunctionORM(BaseORM):
    """
    Table for storing wavefunction data
    """

    __tablename__ = "wavefunction_store"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("singlepoint_record.id", ondelete="cascade"), nullable=False)

    compression_type = Column(Enum(CompressionEnum), nullable=False)
    compression_level = Column(Integer, nullable=False)
    data = deferred(Column(LargeBinary, nullable=False))

    __table_args__ = (UniqueConstraint("record_id", name="ux_wavefunction_store_record_id"),)

    def get_wavefunction(self) -> WavefunctionProperties:
        d = decompress(self.data, self.compression_type)
        return WavefunctionProperties(**d)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "record_id", "compression_level")
        return BaseORM.model_dict(self, exclude)


# Mark the storage of the data_local column as external
event.listen(
    WavefunctionORM.__table__,
    "after_create",
    DDL("ALTER TABLE native_file ALTER COLUMN data SET STORAGE EXTERNAL").execute_if(dialect=("postgresql")),
)
