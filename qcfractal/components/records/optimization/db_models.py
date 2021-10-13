from __future__ import annotations
from sqlalchemy import Column, Integer, ForeignKey, String, JSON, select, func, Index, CheckConstraint
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import relationship, column_property

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseResultORM
from qcfractal.interface.models import ObjectId
from qcfractal.db_socket import BaseORM

from typing import Iterable, Dict, Any, Optional


class Trajectory(BaseORM):
    """Association table for many to many"""

    __tablename__ = "opt_result_association"

    opt_id = Column(Integer, ForeignKey("optimization_procedure.id", ondelete="cascade"), primary_key=True)
    result_id = Column(Integer, ForeignKey("result.id", ondelete="cascade"), primary_key=True)
    position = Column(Integer, primary_key=True)
    # Index('opt_id', 'result_id', unique=True)

    # trajectory_obj = relationship(ResultORM, lazy="noload")


class OptimizationProcedureORM(BaseResultORM):
    """
    An Optimization  procedure
    """

    __tablename__ = "optimization_procedure"

    id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), primary_key=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 1)
        self.procedure = "optimization"
        super().__init__(**kwargs)

    schema_version = Column(Integer, default=1)

    program = Column(String(100), nullable=False)
    keywords = Column(JSON)
    qc_spec = Column(JSON)

    initial_molecule = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    initial_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=initial_molecule)

    # # Results
    energies = Column(JSON)  # Column(ARRAY(Float))
    final_molecule = Column(Integer, ForeignKey("molecule.id"))
    final_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=final_molecule)

    # ids, calculated not stored in this table
    # NOTE: this won't work in SQLite since it returns ARRAYS, aggregate_order_by
    trajectory = column_property(
        select([func.array_agg(aggregate_order_by(Trajectory.result_id, Trajectory.position))])
        .where(Trajectory.opt_id == id)
        .scalar_subquery()
    )

    # array of objects (results) - Lazy - raise error of accessed
    trajectory_obj = relationship(
        Trajectory,
        cascade="all, delete-orphan",
        # backref="optimization_procedure",
        order_by=Trajectory.position,
        collection_class=ordering_list("position"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "optimization_procedure",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }

    __table_args__ = (
        Index("ix_optimization_program", "program"),  # todo: needed for procedures?
        # WARNING - these are not autodetected by alembic
        CheckConstraint("program = LOWER(program)", name="ck_optimization_procedure_program_lower"),
    )

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseORM.dict(self, exclude)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d
