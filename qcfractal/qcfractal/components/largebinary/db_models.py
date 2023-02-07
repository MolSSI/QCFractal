from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, String, Enum, LargeBinary, BigInteger, event, DDL
from sqlalchemy.orm import deferred

from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum

if TYPE_CHECKING:
    pass


class LargeBinaryORM(BaseORM):
    """
    Table for storing large amounts of binary data
    """

    __tablename__ = "largebinary_store"

    id = Column(Integer, primary_key=True)
    largebinary_type = Column(String, nullable=False)

    size = Column(BigInteger, nullable=False)
    checksum = Column(String, nullable=False)
    compression_type = Column(Enum(CompressionEnum), nullable=False)

    # This column is marked as STORAGE EXTERNAL with an event below, which disables compression
    data_local = deferred(Column(LargeBinary, nullable=False))

    __mapper_args__ = {"polymorphic_on": "largebinary_type", "polymorphic_identity": "generic"}


# Mark the storage of the data_local column as external
event.listen(
    LargeBinaryORM.__table__,
    "after_create",
    DDL("ALTER TABLE largebinary_store ALTER COLUMN data_local SET STORAGE EXTERNAL").execute_if(
        dialect=("postgresql")
    ),
)
