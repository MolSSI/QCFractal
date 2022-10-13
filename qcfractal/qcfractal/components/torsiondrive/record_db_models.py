from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select, Column, Integer, ForeignKey, String, UniqueConstraint, Index, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.optimization.record_db_models import (
    OptimizationSpecificationORM,
    OptimizationRecordORM,
)
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class TorsiondriveOptimizationORM(BaseORM):
    """
    Table for storing torsiondrive to optimization relationships
    """

    __tablename__ = "torsiondrive_optimization"

    torsiondrive_id = Column(Integer, ForeignKey("torsiondrive_record.id", ondelete="cascade"), primary_key=True)
    optimization_id = Column(Integer, ForeignKey(OptimizationRecordORM.id), primary_key=True)
    key = Column(String, nullable=False, primary_key=True)
    position = Column(Integer, primary_key=True)

    energy = column_property(
        select(OptimizationRecordORM.energies[-1]).where(OptimizationRecordORM.id == optimization_id).scalar_subquery()
    )

    optimization_record = relationship(OptimizationRecordORM)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "torsiondrive_id")
        return BaseORM.model_dict(self, exclude)


class TorsiondriveInitialMoleculeORM(BaseORM):
    """
    Table for storing torsiondrive to initial molecule relationships
    """

    __tablename__ = "torsiondrive_initial_molecule"

    torsiondrive_id = Column(Integer, ForeignKey("torsiondrive_record.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column("molecule_id", Integer, ForeignKey(MoleculeORM.id), primary_key=True)

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "torsiondrive_id")
        return BaseORM.model_dict(self, exclude)


class TorsiondriveSpecificationORM(BaseORM):
    """
    Table for storing torsiondrive specifications
    """

    __tablename__ = "torsiondrive_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)

    optimization_specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=False)
    optimization_specification = relationship(OptimizationSpecificationORM, lazy="joined")

    keywords = Column(JSONB, nullable=False)
    keywords_hash = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program",
            "optimization_specification_id",
            "keywords_hash",
            name="ux_torsiondrive_specification_keys",
        ),
        Index("ix_torsiondrive_specification_program", "program"),
        Index("ix_torsiondrive_specification_optimization_specification_id", "optimization_specification_id"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_torsiondrive_specification_program_lower"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "keywords_hash", "optimization_specification_id")
        return BaseORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        r = {self.program: None}
        r.update(self.optimization_specification.required_programs)
        return r


class TorsiondriveRecordORM(BaseRecordORM):
    """
    Table for storing torsiondrive calculations
    """

    __tablename__ = "torsiondrive_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(TorsiondriveSpecificationORM.id), nullable=False)
    specification = relationship(TorsiondriveSpecificationORM, lazy="selectin")

    initial_molecules = relationship(MoleculeORM, secondary=TorsiondriveInitialMoleculeORM.__table__)

    optimizations = relationship(
        TorsiondriveOptimizationORM,
        order_by=TorsiondriveOptimizationORM.position,
        collection_class=ordering_list("position"),
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "torsiondrive",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
