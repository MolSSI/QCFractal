from __future__ import annotations

from sqlalchemy import Column, Integer, ForeignKey, String, JSON
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseResultORM
from qcfractal.db_socket import BaseORM

from typing import Dict, Any, Optional, Iterable


class GridOptimizationAssociation(BaseORM):
    """Association table for many to many"""

    __tablename__ = "grid_optimization_association"

    grid_opt_id = Column(Integer, ForeignKey("grid_optimization_procedure.id", ondelete="cascade"), primary_key=True)
    key = Column(String, nullable=False, primary_key=True)

    # not primary key
    opt_id = Column(Integer, ForeignKey("optimization_procedure.id", ondelete="cascade"))

    # Index('grid_opt_id', 'key', unique=True)

    # optimization_obj = relationship(OptimizationProcedureORM, lazy="joined")


class GridOptimizationProcedureORM(BaseResultORM):

    __tablename__ = "grid_optimization_procedure"

    id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), primary_key=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 1)
        kwargs.setdefault("procedure", "gridoptimization")
        super().__init__(**kwargs)

    keywords = Column(JSON)
    qc_spec = Column(JSON)

    # Input data
    initial_molecule = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    initial_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=initial_molecule)

    optimization_spec = Column(JSON)

    # Output data
    starting_molecule = Column(Integer, ForeignKey("molecule.id"))
    starting_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=starting_molecule)

    final_energy_dict = Column(JSON)  # Dict[str, float]
    starting_grid = Column(JSON)  # tuple

    grid_optimizations_obj = relationship(
        GridOptimizationAssociation,
        lazy="select",
        cascade="all, delete-orphan",
    )

    @hybrid_property
    def grid_optimizations(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation in the form of a dict"""

        return self._grid_optimizations(self.grid_optimizations_obj)

    @staticmethod
    def _grid_optimizations(grid_optimizations_obj):

        if not grid_optimizations_obj:
            return {}

        if not isinstance(grid_optimizations_obj, list):
            grid_optimizations_obj = [grid_optimizations_obj]

        ret = {}
        try:
            for obj in grid_optimizations_obj:
                ret[obj.key] = str(obj.opt_id)

        except Exception as err:
            # raises exception of first access!!
            pass
            # print(err)

        return ret

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = BaseResultORM.dict(self, exclude)

        # Always include grid optimizations field
        d["grid_optimizations"] = self.grid_optimizations
        return d

    __table_args__ = ()

    __mapper_args__ = {
        "polymorphic_identity": "grid_optimization_procedure",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }
