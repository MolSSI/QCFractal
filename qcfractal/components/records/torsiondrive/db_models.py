from sqlalchemy import Column, Integer, ForeignKey, String, JSON, select, func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import column_property, relationship

from qcfractal.components.molecules.db_models import MoleculeORM
from qcfractal.components.records.db_models import BaseResultORM
from qcfractal.db_socket import BaseORM
from typing import Optional, Iterable, Any, Dict


class OptimizationHistory(BaseORM):
    """Association table for many to many"""

    __tablename__ = "optimization_history"

    torsion_id = Column(Integer, ForeignKey("torsiondrive_procedure.id", ondelete="cascade"), primary_key=True)
    opt_id = Column(Integer, ForeignKey("optimization_record.id", ondelete="cascade"), primary_key=True)
    key = Column(String, nullable=False, primary_key=True)
    position = Column(Integer, primary_key=True)
    # Index('torsion_id', 'key', unique=True)

    # optimization_obj = relationship(OptimizationProcedureORM, lazy="joined")


class TorsionInitMol(BaseORM):
    """
    Association table for many to many relation
    """

    __tablename__ = "torsion_init_mol_association"

    torsion_id = Column(
        "torsion_id", Integer, ForeignKey("torsiondrive_procedure.id", ondelete="cascade"), primary_key=True
    )
    molecule_id = Column("molecule_id", Integer, ForeignKey("molecule.id", ondelete="cascade"), primary_key=True)


class TorsionDriveProcedureORM(BaseResultORM):
    """
    A torsion drive  procedure
    """

    __tablename__ = "torsiondrive_procedure"

    id = Column(Integer, ForeignKey(BaseResultORM.id, ondelete="cascade"), primary_key=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 1)
        self.procedure = "torsiondrive"
        super().__init__(**kwargs)

    keywords = Column(JSON)
    qc_spec = Column(JSON)

    # ids of the many to many relation
    initial_molecule = column_property(
        select([func.array_agg(TorsionInitMol.molecule_id)]).where(TorsionInitMol.torsion_id == id).scalar_subquery()
    )
    # actual objects relation M2M, never loaded here
    initial_molecule_obj = relationship(MoleculeORM, secondary=TorsionInitMol.__table__, uselist=True, lazy="select")

    optimization_spec = Column(JSON)

    # Output data
    final_energy_dict = Column(JSON)
    minimum_positions = Column(JSON)

    optimization_history_obj = relationship(
        OptimizationHistory,
        cascade="all, delete-orphan",  # backref="torsiondrive_procedure",
        order_by=OptimizationHistory.position,
        collection_class=ordering_list("position"),
        lazy="select",
    )

    @hybrid_property
    def optimization_history(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation in the form of a dict"""

        return self._optimization_history(self.optimization_history_obj)

    @staticmethod
    def _optimization_history(optimization_history_obj):

        if not optimization_history_obj:
            return {}

        if not isinstance(optimization_history_obj, list):
            optimization_history_obj = [optimization_history_obj]

        ret = {}
        try:
            for opt_history in optimization_history_obj:
                if opt_history.key in ret:
                    ret[opt_history.key].append(str(opt_history.opt_id))
                else:
                    ret[opt_history.key] = [str(opt_history.opt_id)]

        except Exception as err:
            # raises exception of first access!!
            pass
            # print(err)

        return ret

    __table_args__ = ()

    __mapper_args__ = {
        "polymorphic_identity": "torsiondrive",
    }

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseResultORM.dict(self, exclude)

        # Always include optimization history
        d["optimization_history"] = self.optimization_history
        return d
