from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, ForeignKey, String, JSON, Index, CheckConstraint, UniqueConstraint, event, DDL
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.record_db_models import QCSpecificationORM, SinglepointRecordORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class OptimizationTrajectoryORM(BaseORM):
    """
    Table for storing optimization to singlepoint relationships (trajectory)
    """

    __tablename__ = "optimization_trajectory"

    optimization_id = Column(Integer, ForeignKey("optimization_record.id", ondelete="cascade"), primary_key=True)
    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), primary_key=True)
    position = Column(Integer, primary_key=True)

    singlepoint_record = relationship(SinglepointRecordORM)

    __table_args__ = (Index("ix_optimization_trajectory_singlepoint_id", "singlepoint_id"),)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "optimization_id", "position")
        return BaseORM.model_dict(self, exclude)


class OptimizationSpecificationORM(BaseORM):
    """
    Table for storing optimization specifications
    """

    __tablename__ = "optimization_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)

    qc_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=False)
    qc_specification = relationship(QCSpecificationORM, lazy="joined")

    keywords = Column(JSONB, nullable=False)
    keywords_hash = Column(String, nullable=False)

    protocols = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program",
            "qc_specification_id",
            "keywords_hash",
            "protocols",
            name="ux_optimization_specification_keys",
        ),
        Index("ix_optimization_specification_program", "program"),
        Index("ix_optimization_specification_qc_specification_id", "qc_specification_id"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_optimization_specification_program_lower"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "keywords_hash", "qc_specification_id")
        return BaseORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        r = {self.program: None}
        r.update(self.qc_specification.required_programs)
        return r

    @property
    def short_description(self) -> str:
        return f"{self.program}+{self.qc_specification.short_description}"


class OptimizationRecordORM(BaseRecordORM):
    """
    Table for storing optimization calculations
    """

    __tablename__ = "optimization_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=False)
    specification = relationship(OptimizationSpecificationORM, lazy="selectin")

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    initial_molecule = relationship(MoleculeORM, foreign_keys=initial_molecule_id)

    final_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=True)
    final_molecule = relationship(MoleculeORM, foreign_keys=final_molecule_id)

    energies = Column(JSON)

    trajectory = relationship(
        OptimizationTrajectoryORM,
        order_by=OptimizationTrajectoryORM.position,
        collection_class=ordering_list("position"),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __mapper_args__ = {"polymorphic_identity": "optimization"}

    __table_args__ = (
        Index("ix_optimization_record_specification_id", "specification_id"),
        Index("ix_optimization_record_initial_molecule_id", "initial_molecule_id"),
        Index("ix_optimization_record_final_molecule_id", "final_molecule_id"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        programs = self.specification.required_programs
        programs["qcengine"] = None
        return programs

    @property
    def short_description(self) -> str:
        return f'{self.initial_molecule.identifiers["molecular_formula"]} {self.specification.short_description}'


# Delete base record if this record is deleted
_del_baserecord_trigger = DDL(
    """
    CREATE TRIGGER qca_optimization_record_delete_base_tr
    AFTER DELETE ON optimization_record
    FOR EACH ROW EXECUTE PROCEDURE qca_base_record_delete();
    """
)

event.listen(
    OptimizationRecordORM.__table__, "after_create", _del_baserecord_trigger.execute_if(dialect=("postgresql"))
)
