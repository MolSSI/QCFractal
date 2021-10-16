from typing import Dict, Any, Optional, Iterable

from sqlalchemy import Column, Integer, Enum, JSON, LargeBinary

from qcfractal.db_socket import BaseORM
from qcfractal.portal.components.outputstore.models import CompressionEnum


class OutputStoreORM(BaseORM):
    __tablename__ = "output_store"

    id = Column(Integer, primary_key=True)
    compression = Column(Enum(CompressionEnum), nullable=True)
    compression_level = Column(Integer, nullable=True)
    value = Column(JSON, nullable=True)
    data = Column(LargeBinary, nullable=True)

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseORM.dict(self, exclude)

        # Old way: store a plain string or dict in "value"
        # New way: store (possibly) compressed output in "data"
        val = d.pop("value")

        # If stored the old way, convert to the new way
        if d["data"] is None:
            # Set the data field to be the string or dictionary
            d["data"] = val

            # Remove these and let the model handle the defaults
            d.pop("compression")
            d.pop("compression_level")

        return d
