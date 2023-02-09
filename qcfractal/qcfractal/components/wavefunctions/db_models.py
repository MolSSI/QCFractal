from __future__ import annotations

from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint, DDL, event

from qcfractal.components.largebinary.db_models import LargeBinaryORM


class WavefunctionORM(LargeBinaryORM):
    """
    Table for storing wavefunction data
    """

    __tablename__ = "wavefunction"

    id = Column(Integer, ForeignKey(LargeBinaryORM.id, ondelete="cascade"), primary_key=True)
    record_id = Column(Integer, ForeignKey("singlepoint_record.id", ondelete="cascade"), nullable=False)

    __table_args__ = (UniqueConstraint("record_id", name="ux_wavefunction_record_id"),)

    __mapper_args__ = {"polymorphic_identity": "wavefunction"}


# Trigger for deleting native files rows when records are deleted
_del_lb_trigger = DDL(
    """
    CREATE TRIGGER qca_wavefunction_delete_lb_tr
    AFTER DELETE ON wavefunction
    FOR EACH ROW EXECUTE PROCEDURE qca_largebinary_base_delete();
    """
)

event.listen(WavefunctionORM.__table__, "after_create", _del_lb_trigger.execute_if(dialect=("postgresql")))
