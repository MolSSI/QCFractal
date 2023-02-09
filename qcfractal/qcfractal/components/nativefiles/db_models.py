from __future__ import annotations

from sqlalchemy import Column, String, Integer, ForeignKey, UniqueConstraint, DDL, event

from qcfractal.components.largebinary.db_models import LargeBinaryORM


class NativeFileORM(LargeBinaryORM):
    """
    Table for storing raw, program-dependent raw data
    """

    __tablename__ = "native_file"

    id = Column(Integer, ForeignKey(LargeBinaryORM.id, ondelete="cascade"), primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)

    name = Column(String, nullable=False)

    __table_args__ = (UniqueConstraint("record_id", "name", name="ux_native_file_record_id_name"),)

    __mapper_args__ = {"polymorphic_identity": "nativefile"}


# Trigger for deleting native files rows when records are deleted
_del_lb_trigger = DDL(
    """
    CREATE TRIGGER qca_native_file_delete_lb_tr
    AFTER DELETE ON native_file
    FOR EACH ROW EXECUTE PROCEDURE qca_largebinary_base_delete();
    """
)

event.listen(NativeFileORM.__table__, "after_create", _del_lb_trigger.execute_if(dialect=("postgresql")))
