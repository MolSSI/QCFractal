from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select, UniqueConstraint, Index, CheckConstraint, Column, Integer, ForeignKey, String, JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.optimization.db_models import OptimizationSpecificationORM, OptimizationRecordORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class GridoptimizationOptimizationORM(BaseORM):
    """Association table for many to many"""

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

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "gridoptimization_id")
        return BaseORM.model_dict(self, exclude)


class GridoptimizationSpecificationORM(BaseORM):
    __tablename__ = "gridoptimization_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)

    optimization_specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=False)
    optimization_specification = relationship(OptimizationSpecificationORM, lazy="joined")

    keywords = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program",
            "optimization_specification_id",
            "keywords",
            name="ux_gridoptimization_specification_keys",
        ),
        Index("ix_gridoptimization_specification_program", "program"),
        Index("ix_gridoptimization_specification_optimization_specification_id", "optimization_specification_id"),
        Index("ix_gridoptimization_specification_keywords", "keywords"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_gridoptimization_specification_program_lower"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "optimization_specification_id")
        return BaseORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        r = {self.program: None}
        r.update(self.optimization_specification.required_programs)
        return r


class GridoptimizationRecordORM(BaseRecordORM):

    __tablename__ = "gridoptimization_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(GridoptimizationSpecificationORM.id), nullable=False)
    specification = relationship(GridoptimizationSpecificationORM, lazy="selectin")

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    initial_molecule = relationship(MoleculeORM, foreign_keys=initial_molecule_id)

    starting_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=True)
    starting_molecule = relationship(MoleculeORM, foreign_keys=starting_molecule_id)

    starting_grid = Column(JSON)  # tuple

    optimizations = relationship(
        GridoptimizationOptimizationORM,
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "gridoptimization",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
