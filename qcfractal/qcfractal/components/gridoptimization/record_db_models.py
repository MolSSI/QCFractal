from __future__ import annotations

from sqlalchemy import (
    select,
    UniqueConstraint,
    Index,
    CheckConstraint,
    Column,
    Integer,
    ForeignKey,
    String,
    JSON,
    event,
    DDL,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.optimization.record_db_models import (
    OptimizationSpecificationORM,
    OptimizationRecordORM,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket import BaseORM


class GridoptimizationOptimizationORM(BaseORM):
    """
    Table for storing gridoptimization to optimization relationships
    """

    __tablename__ = "gridoptimization_optimization"

    gridoptimization_id = Column(
        Integer, ForeignKey("gridoptimization_record.id", ondelete="cascade"), primary_key=True
    )
    optimization_id = Column(Integer, ForeignKey("optimization_record.id"), nullable=False)
    key = Column(String, nullable=False, primary_key=True)

    energy = column_property(
        select(OptimizationRecordORM.energies[-1]).where(OptimizationRecordORM.id == optimization_id).scalar_subquery()
    )

    optimization_record = relationship(OptimizationRecordORM)

    __table_args__ = (Index("ix_gridoptimization_optimization_id", "optimization_id"),)

    _qcportal_model_excludes = ["gridoptimization_id"]


class GridoptimizationSpecificationORM(BaseORM):
    """
    Table for storing gridoptimization specifications
    """

    __tablename__ = "gridoptimization_specification"

    id = Column(Integer, primary_key=True)
    specification_hash = Column(String, nullable=False)

    program = Column(String(100), nullable=False)

    optimization_specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=False)
    optimization_specification = relationship(OptimizationSpecificationORM, lazy="joined")

    keywords = Column(JSONB, nullable=False)
    protocols = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "specification_hash", "optimization_specification_id", name="ux_gridoptimization_specification_keys"
        ),
        Index("ix_gridoptimization_specification_program", "program"),
        Index("ix_gridoptimization_specification_optimization_specification_id", "optimization_specification_id"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_gridoptimization_specification_program_lower"),
    )

    # TODO - protocols will be in model eventually
    _qcportal_model_excludes = [
        "id",
        "specification_hash",
        "optimization_specification_id",
        "protocols",
    ]

    @property
    def short_description(self) -> str:
        return f"{self.program}~{self.optimization_specification.short_description}"


class GridoptimizationRecordORM(BaseRecordORM):
    """
    Table for storing gridoptimization calculations
    """

    __tablename__ = "gridoptimization_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(GridoptimizationSpecificationORM.id), nullable=False)
    specification = relationship(GridoptimizationSpecificationORM, lazy="selectin")

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    initial_molecule = relationship(MoleculeORM, foreign_keys=initial_molecule_id)

    starting_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=True)
    starting_molecule = relationship(MoleculeORM, foreign_keys=starting_molecule_id)

    starting_grid = Column(JSON)  # tuple

    optimizations = relationship(GridoptimizationOptimizationORM, cascade="all, delete-orphan", passive_deletes=True)

    __mapper_args__ = {
        "polymorphic_identity": "gridoptimization",
    }

    _qcportal_model_excludes = [*BaseRecordORM._qcportal_model_excludes, "specification_id"]

    @property
    def short_description(self) -> str:
        return f'{self.initial_molecule.identifiers["molecular_formula"]} {self.specification.short_description}'


# Delete base record if this record is deleted
_del_baserecord_trigger = DDL(
    """
    CREATE TRIGGER qca_gridoptimization_record_delete_base_tr
    AFTER DELETE ON gridoptimization_record
    FOR EACH ROW EXECUTE PROCEDURE qca_base_record_delete();
    """
)

event.listen(
    GridoptimizationRecordORM.__table__, "after_create", _del_baserecord_trigger.execute_if(dialect=("postgresql"))
)
