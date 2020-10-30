import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB, aggregate_order_by
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import column_property, relationship

from qcfractal.interface.models.records import DriverEnum, RecordStatusEnum
from qcfractal.storage_sockets.models.sql_base import Base, MsgpackExt
from qcfractal.storage_sockets.models.sql_models import KeywordsORM, KVStoreORM, MoleculeORM


class BaseResultORM(Base):
    """
    Abstract Base class for ResultORMs and ProcedureORMs
    """

    __tablename__ = "base_result"

    # for SQL
    result_type = Column(String)  # for inheritance

    # Base identification
    id = Column(Integer, primary_key=True)
    # ondelete="SET NULL": when manger is deleted, set this field to None
    manager_name = Column(String, ForeignKey("queue_manager.name", ondelete="SET NULL"), nullable=True)

    hash_index = Column(String)  # TODO
    procedure = Column(String(100), nullable=False)  # TODO: may remove
    version = Column(Integer)
    protocols = Column(JSONB, nullable=False)

    # Extra fields
    extras = Column(MsgpackExt)
    stdout = Column(Integer, ForeignKey("kv_store.id"))
    stdout_obj = relationship(
        KVStoreORM, lazy="noload", foreign_keys=stdout, cascade="all, delete-orphan", single_parent=True
    )

    stderr = Column(Integer, ForeignKey("kv_store.id"))
    stderr_obj = relationship(
        KVStoreORM, lazy="noload", foreign_keys=stderr, cascade="all, delete-orphan", single_parent=True
    )

    error = Column(Integer, ForeignKey("kv_store.id"))
    error_obj = relationship(
        KVStoreORM, lazy="noload", foreign_keys=error, cascade="all, delete-orphan", single_parent=True
    )

    # Compute status
    status = Column(Enum(RecordStatusEnum), nullable=False, default=RecordStatusEnum.incomplete)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # Carry-ons
    provenance = Column(JSON)

    __table_args__ = (
        Index("ix_base_result_status", "status"),
        Index("ix_base_result_type", "result_type"),  # todo: needed?
    )

    __mapper_args__ = {"polymorphic_on": "result_type"}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class WavefunctionStoreORM(Base):

    __tablename__ = "wavefunction_store"

    id = Column(Integer, primary_key=True)

    # Sparsity is very cheap
    basis = Column(MsgpackExt, nullable=False)
    restricted = Column(Boolean, nullable=False)

    # Core Hamiltonian
    h_core_a = Column(MsgpackExt, nullable=True)
    h_core_b = Column(MsgpackExt, nullable=True)
    h_effective_a = Column(MsgpackExt, nullable=True)
    h_effective_b = Column(MsgpackExt, nullable=True)

    # SCF Results
    scf_orbitals_a = Column(MsgpackExt, nullable=True)
    scf_orbitals_b = Column(MsgpackExt, nullable=True)
    scf_density_a = Column(MsgpackExt, nullable=True)
    scf_density_b = Column(MsgpackExt, nullable=True)
    scf_fock_a = Column(MsgpackExt, nullable=True)
    scf_fock_b = Column(MsgpackExt, nullable=True)
    scf_eigenvalues_a = Column(MsgpackExt, nullable=True)
    scf_eigenvalues_b = Column(MsgpackExt, nullable=True)
    scf_occupations_a = Column(MsgpackExt, nullable=True)
    scf_occupations_b = Column(MsgpackExt, nullable=True)

    # Extras
    extras = Column(MsgpackExt, nullable=True)


class ResultORM(BaseResultORM):
    """
    Hold the result of an atomic single calculation
    """

    __tablename__ = "result"

    id = Column(Integer, ForeignKey("base_result.id", ondelete="CASCADE"), primary_key=True)

    # uniquely identifying a result
    program = Column(String(100), nullable=False)  # example "rdkit", is it the same as program in keywords?
    driver = Column(String(100), Enum(DriverEnum), nullable=False)
    method = Column(String(100), nullable=False)  # example "uff"
    basis = Column(String(100))
    molecule = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    molecule_obj = relationship(MoleculeORM, lazy="select")

    # This is a special case where KeywordsORM are denormalized intentionally as they are part of the
    # lookup for a single result and querying a result will not often request the keywords (LazyReference)
    keywords = Column(Integer, ForeignKey("keywords.id"))
    keywords_obj = relationship(KeywordsORM, lazy="select")

    # Primary Result output
    return_result = Column(MsgpackExt)
    properties = Column(JSON)  # TODO: may use JSONB in the future

    # Wavefunction data
    wavefunction = Column(JSONB, nullable=True)
    wavefunction_data_id = Column(Integer, ForeignKey("wavefunction_store.id"), nullable=True)
    wavefunction_data_obj = relationship(
        WavefunctionStoreORM,
        lazy="noload",
        foreign_keys=wavefunction_data_id,
        cascade="all, delete-orphan",
        single_parent=True,
    )

    __table_args__ = (
        # TODO: optimize indexes
        # A multicolumn GIN index can be used with query conditions that involve any subset of
        # the index's columns. Unlike B-tree or GiST, index search effectiveness is the same
        # regardless of which index column(s) the query conditions use.
        # Index('ix_result_combined', "program", "driver", "method", "basis",
        #       "keywords", postgresql_using='gin'),  # gin index
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name="uix_results_keys"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "result",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ProcedureMixin:
    """
    A procedure mixin to be used by specific procedure types
    """

    program = Column(String(100), nullable=False)
    keywords = Column(JSON)
    qc_spec = Column(JSON)


# ================== Types of ProcedureORMs ================== #


class Trajectory(Base):
    """Association table for many to many"""

    __tablename__ = "opt_result_association"

    opt_id = Column(Integer, ForeignKey("optimization_procedure.id", ondelete="cascade"), primary_key=True)
    result_id = Column(Integer, ForeignKey("result.id", ondelete="cascade"), primary_key=True)
    position = Column(Integer, primary_key=True)
    # Index('opt_id', 'result_id', unique=True)

    # trajectory_obj = relationship(ResultORM, lazy="noload")


class OptimizationProcedureORM(ProcedureMixin, BaseResultORM):
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
    initial_molecule = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    initial_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=initial_molecule)

    # # Results
    energies = Column(JSON)  # Column(ARRAY(Float))
    final_molecule = Column(Integer, ForeignKey("molecule.id"))
    final_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=final_molecule)

    # ids, calculated not stored in this table
    # NOTE: this won't work in SQLite since it returns ARRAYS, aggregate_order_by
    trajectory = column_property(
        select([func.array_agg(aggregate_order_by(Trajectory.result_id, Trajectory.position))]).where(
            Trajectory.opt_id == id
        )
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

    __table_args__ = (Index("ix_optimization_program", "program"),)  # todo: needed for procedures?

    def update_relations(self, trajectory=None, **kwarg):

        # update optimization_results relations
        # self._update_many_to_many(opt_result_association, 'opt_id', 'result_id',
        #                 self.id, trajectory, self.trajectory)

        self.trajectory_obj = []
        trajectory = [] if not trajectory else trajectory
        for result_id in trajectory:
            traj = Trajectory(opt_id=int(self.id), result_id=int(result_id))
            self.trajectory_obj.append(traj)

    # def add_relations(self, trajectory):
    #     session = object_session(self)
    #     # add many to many relation with results if ids are given not objects
    #     if trajectory:
    #         session.execute(
    #             opt_result_association
    #                 .insert()  # or update
    #                 .values([(self.id, i) for i in trajectory])
    #         )
    #     session.commit()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class GridOptimizationAssociation(Base):
    """Association table for many to many"""

    __tablename__ = "grid_optimization_association"

    grid_opt_id = Column(Integer, ForeignKey("grid_optimization_procedure.id", ondelete="cascade"), primary_key=True)
    key = Column(String, nullable=False, primary_key=True)

    # not primary key
    opt_id = Column(Integer, ForeignKey("optimization_procedure.id", ondelete="cascade"))

    # Index('grid_opt_id', 'key', unique=True)

    # optimization_obj = relationship(OptimizationProcedureORM, lazy="joined")


class GridOptimizationProcedureORM(ProcedureMixin, BaseResultORM):

    __tablename__ = "grid_optimization_procedure"

    id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), primary_key=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 1)
        kwargs.setdefault("procedure", "gridoptimization")
        kwargs.setdefault("program", "qcfractal")
        super().__init__(**kwargs)

    # Input data
    initial_molecule = Column(Integer, ForeignKey("molecule.id"), nullable=False)
    initial_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=initial_molecule)

    optimization_spec = Column(JSON)

    # Output data
    starting_molecule = Column(Integer, ForeignKey("molecule.id"))
    starting_molecule_obj = relationship(MoleculeORM, lazy="select", foreign_keys=initial_molecule)

    final_energy_dict = Column(JSON)  # Dict[str, float]
    starting_grid = Column(JSON)  # tuple

    grid_optimizations_obj = relationship(
        GridOptimizationAssociation,
        lazy="selectin",
        cascade="all, delete-orphan",
        backref="grid_optimization_procedure",
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

    @grid_optimizations.setter
    def grid_optimizations(self, dict_values):

        return dict_values

    __table_args__ = (Index("ix_grid_optmization_program", "program"),)  # todo: needed for procedures?

    __mapper_args__ = {
        "polymorphic_identity": "grid_optimization_procedure",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }

    def update_relations(self, grid_optimizations=None, **kwarg):

        self.grid_optimizations_obj = []
        for key, opt_id in grid_optimizations.items():
            obj = GridOptimizationAssociation(grid_opt_id=int(self.id), opt_id=int(opt_id), key=key)
            self.grid_optimizations_obj.append(obj)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class OptimizationHistory(Base):
    """Association table for many to many"""

    __tablename__ = "optimization_history"

    torsion_id = Column(Integer, ForeignKey("torsiondrive_procedure.id", ondelete="cascade"), primary_key=True)
    opt_id = Column(Integer, ForeignKey("optimization_procedure.id", ondelete="cascade"), primary_key=True)
    key = Column(String, nullable=False, primary_key=True)
    position = Column(Integer, primary_key=True)
    # Index('torsion_id', 'key', unique=True)

    # optimization_obj = relationship(OptimizationProcedureORM, lazy="joined")


class TorsionInitMol(Base):
    """
    Association table for many to many relation
    """

    __tablename__ = "torsion_init_mol_association"

    torsion_id = Column(
        "torsion_id", Integer, ForeignKey("torsiondrive_procedure.id", ondelete="cascade"), primary_key=True
    )
    molecule_id = Column("molecule_id", Integer, ForeignKey("molecule.id", ondelete="cascade"), primary_key=True)


class TorsionDriveProcedureORM(ProcedureMixin, BaseResultORM):
    """
    A torsion drive  procedure
    """

    __tablename__ = "torsiondrive_procedure"

    id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), primary_key=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 1)
        self.procedure = "torsiondrive"
        self.program = "torsiondrive"
        super().__init__(**kwargs)

    # input data (along with the mixin)

    # ids of the many to many relation
    initial_molecule = column_property(
        select([func.array_agg(TorsionInitMol.molecule_id)]).where(TorsionInitMol.torsion_id == id)
    )
    # actual objects relation M2M, never loaded here
    initial_molecule_obj = relationship(MoleculeORM, secondary=TorsionInitMol.__table__, uselist=True, lazy="noload")

    optimization_spec = Column(JSON)

    # Output data
    final_energy_dict = Column(JSON)
    minimum_positions = Column(JSON)

    optimization_history_obj = relationship(
        OptimizationHistory,
        cascade="all, delete-orphan",  # backref="torsiondrive_procedure",
        order_by=OptimizationHistory.position,
        collection_class=ordering_list("position"),
        lazy="selectin",
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

    @optimization_history.setter
    def optimization_history(self, dict_values):
        """A private copy of the opt history as a dict
        Key: list of optimization procedures"""

        return dict_values

    __table_args__ = (Index("ix_torsion_drive_program", "program"),)  # todo: needed for procedures?

    __mapper_args__ = {
        "polymorphic_identity": "torsiondrive_procedure",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }

    def update_relations(self, initial_molecule=None, optimization_history=None, **kwarg):

        # update torsion molecule relation
        self._update_many_to_many(
            TorsionInitMol.__table__, "torsion_id", "molecule_id", self.id, initial_molecule, self.initial_molecule
        )

        self.optimization_history_obj = []
        for key in optimization_history:
            for opt_id in optimization_history[key]:
                opt_history = OptimizationHistory(torsion_id=int(self.id), opt_id=int(opt_id), key=key)
                self.optimization_history_obj.append(opt_history)

        # No need for the following because the session is committed with parent save
        # session.add_all(self.optimization_history_obj)
        # session.add(self)
        # session.commit()


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
