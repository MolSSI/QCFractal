from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, ForeignKey, String, JSON, Index, CheckConstraint, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.singlepoint.db_models import SinglepointSpecificationORM, SinglepointRecordORM
from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    pass


class OptimizationTrajectoryORM(BaseORM):

    __tablename__ = "optimization_trajectory"

    optimization_id = Column(Integer, ForeignKey("optimization_record.id", ondelete="cascade"), primary_key=True)
    singlepoint_id = Column(Integer, ForeignKey(SinglepointRecordORM.id), primary_key=True)
    position = Column(Integer, primary_key=True)

    singlepoint_record = relationship(SinglepointRecordORM)
    optimization_record = relationship("OptimizationRecordORM")


class OptimizationSpecificationORM(BaseORM):
    __tablename__ = "optimization_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)

    singlepoint_specification_id = Column(Integer, ForeignKey(SinglepointSpecificationORM.id), nullable=False)
    singlepoint_specification = relationship(SinglepointSpecificationORM, lazy="selectin", uselist=False)

    keywords = Column(JSONB, nullable=False)
    protocols = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program",
            "singlepoint_specification_id",
            "keywords",
            "protocols",
            name="ux_optimization_specification_keys",
        ),
        Index("ix_optimization_specification_program", "program"),
        Index("ix_optimization_specification_singlepoint_specification_id", "singlepoint_specification_id"),
        Index("ix_optimization_specification_keywords", "keywords"),
        Index("ix_optimization_specification_protocols", "protocols"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_optimization_specification_program_lower"),
    )


class OptimizationRecordORM(BaseRecordORM):
    """
    An Optimization  procedure
    """

    __tablename__ = "optimization_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=False)
    specification = relationship(OptimizationSpecificationORM, lazy="selectin")

    initial_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=False)
    initial_molecule = relationship(MoleculeORM, lazy="select", foreign_keys=initial_molecule_id)

    final_molecule_id = Column(Integer, ForeignKey(MoleculeORM.id), nullable=True)
    final_molecule = relationship(MoleculeORM, lazy="select", foreign_keys=final_molecule_id)

    energies = Column(JSON)

    trajectory = relationship(
        OptimizationTrajectoryORM,
        lazy="select",
        order_by=OptimizationTrajectoryORM.position,
        collection_class=ordering_list("position"),
        back_populates="optimization_record",
    )

    __mapper_args__ = {"polymorphic_identity": "optimization"}

    __table_args__ = (
        Index("ix_optimization_record_specification_id", "specification_id"),
        Index("ix_optimization_record_initial_molecule_id", "initial_molecule_id"),
        Index("ix_optimization_record_final_molecule_id", "final_molecule_id"),
    )
