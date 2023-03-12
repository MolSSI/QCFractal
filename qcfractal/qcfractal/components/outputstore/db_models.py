from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import event, DDL

from qcfractal.components.record_db_models import OutputStoreORM

if TYPE_CHECKING:
    pass

# Mark the storage of the data column as external
event.listen(
    OutputStoreORM.__table__,
    "after_create",
    DDL("ALTER TABLE output_store ALTER COLUMN data SET STORAGE EXTERNAL").execute_if(dialect=("postgresql")),
)
