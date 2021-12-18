from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, Enum, JSON, LargeBinary, ForeignKey, Index, UniqueConstraint

from qcfractal.db_socket import BaseORM
from qcfractal.portal.outputstore import CompressionEnum, OutputTypeEnum, OutputStore

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class OutputStoreORM(BaseORM):
    __tablename__ = "output_store"

    id = Column(Integer, primary_key=True)
    history_id = Column(Integer, ForeignKey("record_compute_history.id", ondelete="CASCADE"), nullable=False)

    output_type = Column(Enum(OutputTypeEnum), nullable=False)
    compression = Column(Enum(CompressionEnum), nullable=True)
    compression_level = Column(Integer, nullable=True)
    value = Column(JSON, nullable=True)
    data = Column(LargeBinary, nullable=True)

    __table_args__ = (
        Index("ix_output_store_history_id", "history_id"),
        UniqueConstraint("history_id", "output_type", name="ux_output_store_id_type"),
    )

    @classmethod
    def from_model(cls, output_model: OutputStore):
        return cls(**output_model.dict())

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseORM.dict(self, exclude)

        # Old way: store a plain string or dict in "value"
        # New way: store (possibly) compressed output in "data"
        val = d.pop("value", None)

        # If stored the old way, convert to the new way
        if d["data"] is None:
            # Set the data field to be the string or dictionary
            d["data"] = val

            # Remove these and let the model handle the defaults
            d.pop("compression")
            d.pop("compression_level")

        return d

    def append(self, to_append: str):
        out_obj = OutputStore(**self.dict())
        new_str = out_obj.get_string() + to_append

        new_obj = OutputStore.compress(self.output_type, new_str, self.compression, self.compression_level)

        self.value = None
        self.data = new_obj.data
        self.compression = new_obj.compression
        self.compression_level = new_obj.compression_level
