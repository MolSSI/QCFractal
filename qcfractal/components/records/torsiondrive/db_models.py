from __future__ import annotations

from typing import Dict, Optional

from sqlalchemy import Column, Integer, ForeignKey, String, JSON, UniqueConstraint, Index, CheckConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseRecordORM
from qcfractal.components.records.optimization.db_models import OptimizationSpecificationORM
from qcfractal.db_socket import BaseORM


class TorsiondriveOptimizationHistoryORM(BaseORM):
    """Association table for many to many"""

    __tablename__ = "torsiondrive_optimizations"

    torsiondrive_id = Column(Integer, ForeignKey("torsiondrive_record.id", ondelete="cascade"), primary_key=True)
    optimization_id = Column(Integer, ForeignKey("optimization_record.id"), primary_key=True)
    key = Column(String, nullable=False, primary_key=True)
    position = Column(Integer, primary_key=True)


class TorsiondriveInitialMoleculeORM(BaseORM):
    """
    Association table torsiondrive -> initial molecules
    """

    __tablename__ = "torsiondrive_initial_molecules"

    torsiondrive_id = Column(Integer, ForeignKey("torsiondrive_record.id", ondelete="cascade"), primary_key=True)
    molecule_id = Column("molecule_id", Integer, ForeignKey(MoleculeORM.id), primary_key=True)


class TorsiondriveSpecificationORM(BaseORM):
    __tablename__ = "torsiondrive_specification"

    id = Column(Integer, primary_key=True)

    program = Column(String(100), nullable=False)

    optimization_specification_id = Column(Integer, ForeignKey(OptimizationSpecificationORM.id), nullable=False)
    optimization_specification = relationship(OptimizationSpecificationORM, lazy="selectin", uselist=False)

    keywords = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "program",
            "optimization_specification_id",
            "keywords",
            name="ux_torsiondrive_specification_keys",
        ),
        Index("ix_torsiondrive_specification_program", "program"),
        Index("ix_torsiondrive_specification_optimization_specification_id", "optimization_specification_id"),
        Index("ix_torsiondrive_specification_keywords", "keywords"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_torsiondrive_specification_program_lower"),
    )

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        r = {self.program: None}
        r.update(self.optimization_specification.required_programs)
        return r


class TorsiondriveRecordORM(BaseRecordORM):
    """
    A torsion drive procedure
    """

    __tablename__ = "torsiondrive_record"

    id = Column(Integer, ForeignKey(BaseRecordORM.id, ondelete="cascade"), primary_key=True)

    specification_id = Column(Integer, ForeignKey(TorsiondriveSpecificationORM.id), nullable=False)
    specification = relationship(TorsiondriveSpecificationORM, lazy="selectin")

    initial_molecules = relationship(MoleculeORM, secondary=TorsiondriveInitialMoleculeORM.__table__, uselist=True)

    # Output data
    final_energies = Column(JSON)
    minimum_positions = Column(JSON)

    optimization_history = relationship(
        TorsiondriveOptimizationHistoryORM,
        order_by=TorsiondriveOptimizationHistoryORM.position,
        collection_class=ordering_list("position"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "torsiondrive",
    }

    @property
    def required_programs(self) -> Dict[str, Optional[str]]:
        return self.specification.required_programs
