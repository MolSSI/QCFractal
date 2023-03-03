from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, Enum, LargeBinary, ForeignKey, UniqueConstraint, event, DDL, JSON
from sqlalchemy.orm import deferred

from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum
from qcportal.outputstore import OutputTypeEnum

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class OutputStoreORM(BaseORM):
    """
    Table for storing raw computation outputs (text) and errors (json)
    """

    __tablename__ = "output_store"

    id = Column(Integer, primary_key=True)
    history_id = Column(Integer, ForeignKey("record_compute_history.id", ondelete="cascade"), nullable=False)

    output_type = Column(Enum(OutputTypeEnum), nullable=False)
    compression_type = Column(Enum(CompressionEnum), nullable=True)
    compression_level = Column(Integer, nullable=True)
    data = deferred(Column(LargeBinary, nullable=True))

    # unmigrated data
    value = deferred(Column(JSON, nullable=True))
    old_data = deferred(Column(LargeBinary, nullable=True))

    __table_args__ = (UniqueConstraint("history_id", "output_type", name="ux_output_store_id_type"),)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Fields not in model
        exclude = self.append_exclude(exclude, "id", "history_id", "compression_level")

        d = BaseORM.model_dict(self, exclude)

        # TODO - remove after full migration
        if d.get("compression_type", None) is None:
            d["compression_type"] = CompressionEnum.none

        return d


# Mark the storage of the data column as external
event.listen(
    OutputStoreORM.__table__,
    "after_create",
    DDL("ALTER TABLE output_store ALTER COLUMN data SET STORAGE EXTERNAL").execute_if(dialect=("postgresql")),
)
