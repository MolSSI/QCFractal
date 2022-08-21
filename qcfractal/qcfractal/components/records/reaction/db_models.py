from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, String, Integer, ForeignKey, CheckConstraint, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, JSONB
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.optimization.db_models import OptimizationRecordORM, OptimizationSpecificationORM
from qcfractal.components.records.singlepoint.db_models import SinglepointRecordORM, QCSpecificationORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class ReactionComponentORM(BaseORM):
    """
    Table for storing reaction specifications
    """

    __tablename__ = "reaction_component"

    reaction_id = Column(Integer, ForeignKey("reaction_record.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column(Integer, ForeignKey("molecule.id"), primary_key=True)
    coefficient = Column(DOUBLE_PRECISION, nullable=False)

    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), nullable=True)
    optimization_id = Column(Integer, ForeignKey(OptimizationRecordORM.id), nullable=True)

    molecule = relationship(MoleculeORM, lazy="joined")
    singlepoint_record = relationship(SinglepointRecordORM)
    optimization_record = relationship(OptimizationRecordORM)

    __table_args__ = (
        Index("ix_reaction_component_singlepoint_id", "singlepoint_id"),
        Index("ix_reaction_component_optimization_id", "optimization_id"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "reaction_id")
        return BaseORM.model_dict(self, exclude)


class ReactionSpecificationORM(BaseORM):
    """
    Table for storing reaction specifications
    """

    __tablename__ = "reaction_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String, nullable=False)
    singlepoint_specification_id = Column(Integer, ForeignKey(QCSpecificationORM.id), nullable=True)
    optimization_specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=True)
    keywords = Column(JSONB, nullable=False)

    singlepoint_specification = relationship(QCSpecificationORM, lazy="joined")
    optimization_specification = relationship(OptimizationSpecificationORM, lazy="joined")

    __table_args__ = (
        UniqueConstraint(
            "singlepoint_specification_id",
            "optimization_specification_id",
            "keywords",
            name="ux_reaction_specification_keys",
        ),
        Index("ix_reaction_specification_program", "program"),
        Index("ix_reaction_specification_singlepoint_specification_id", "singlepoint_specification_id"),
        Index("ix_reaction_specification_optimization_specification_id", "optimization_specification_id"),
        Index("ix_reaction_specification_keywords", "keywords"),
        CheckConstraint("program = LOWER(program)", name="ck_reaction_specification_program_lower"),
        CheckConstraint(
            "singlepoint_specification_id IS NOT NULL OR optimization_specification_id IS NOT NULL",
            name="ck_reaction_specification_specs",
        ),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "id", "singlepoint_specification_id", "optimization_specification_id")
        return BaseORM.model_dict(self, exclude)


class ReactionRecordORM(BaseRecordORM):
    """
    Table for storing reaction calculations
    """

    __tablename__ = "reaction_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(ReactionSpecificationORM.id), nullable=False)
    specification = relationship(ReactionSpecificationORM, lazy="selectin")

    total_energy = Column(DOUBLE_PRECISION, nullable=True)

    components = relationship(
        ReactionComponentORM,
        cascade="all, delete-orphan",
    )

    __mapper_args__ = {
        "polymorphic_identity": "reaction",
    }

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        # Remove fields not present in the model
        exclude = self.append_exclude(exclude, "specification_id")
        return BaseRecordORM.model_dict(self, exclude)

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
