import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    CheckConstraint,
    func,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB, aggregate_order_by
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import column_property, relationship

from qcfractal.components.wavefunction.db_models import WavefunctionStoreORM
from qcfractal.interface.models import DriverEnum, RecordStatusEnum, ObjectId
from qcfractal.storage_sockets.models.sql_base import Base, MsgpackExt
from qcfractal.components.keywords.db_models import KeywordsORM
from qcfractal.components.outputstore.db_models import KVStoreORM
from qcfractal.components.molecule.db_models import MoleculeORM


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
        KVStoreORM, lazy="select", foreign_keys=stdout, cascade="all, delete-orphan", single_parent=True
    )

    stderr = Column(Integer, ForeignKey("kv_store.id"))
    stderr_obj = relationship(
        KVStoreORM, lazy="select", foreign_keys=stderr, cascade="all, delete-orphan", single_parent=True
    )

    error = Column(Integer, ForeignKey("kv_store.id"))
    error_obj = relationship(
        KVStoreORM, lazy="select", foreign_keys=error, cascade="all, delete-orphan", single_parent=True
    )

    # Compute status
    status = Column(Enum(RecordStatusEnum), nullable=False, default=RecordStatusEnum.waiting)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # Carry-ons
    provenance = Column(JSON)

    # Related task. The foreign key is in the task_queue table
    task_obj = relationship("TaskQueueORM", back_populates="base_result_obj", uselist=False)

    # Related service. The foreign key is in the service_queue table
    service_obj = relationship("ServiceQueueORM", back_populates="procedure_obj", uselist=False)

    __table_args__ = (
        Index("ix_base_result_status", "status"),
        Index("ix_base_result_type", "result_type"),  # todo: needed?
        Index("ix_base_result_stdout", "stdout", unique=True),
        Index("ix_base_result_stderr", "stderr", unique=True),
        Index("ix_base_result_error", "error", unique=True),
        Index("ix_base_result_hash_index", "hash_index", unique=False),
    )

    __mapper_args__ = {"polymorphic_on": "result_type"}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


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
        lazy="select",
        foreign_keys=wavefunction_data_id,
        cascade="all, delete-orphan",
        single_parent=True,
    )

    __table_args__ = (
        # We use simple multi-column constraint, then add hash indices to the various columns
        UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name="uix_results_keys"),
        # Enforce lowercase on some fields
        # This does not actually change the text to lowercase, but will fail to insert anything not lowercase
        # WARNING - these are not autodetected by alembic
        Index("ix_results_program", "program"),
        Index("ix_results_driver", "driver"),
        Index("ix_results_method", "method"),
        Index("ix_results_basis", "basis"),
        Index("ix_results_keywords", "keywords"),
        Index("ix_results_molecule", "molecule"),
        CheckConstraint("program = LOWER(program)", name="ck_result_program_lower"),
        CheckConstraint("driver = LOWER(driver)", name="ck_result_driver_lower"),
        CheckConstraint("method = LOWER(method)", name="ck_result_method_lower"),
        CheckConstraint("basis = LOWER(basis)", name="ck_result_basis_lower"),
    )

    __mapper_args__ = {
        "polymorphic_identity": "result",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }

    def dict(self):

        d = Base.dict(self)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d


# ================== Types of ProcedureORMs ================== #


class Trajectory(Base):
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

    def dict(self):

        d = Base.dict(self)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d


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

    def dict(self):
        d = BaseResultORM.dict(self)

        # Always include grid optimizations field
        d["grid_optimizations"] = self.grid_optimizations
        return d

    __table_args__ = ()

    __mapper_args__ = {
        "polymorphic_identity": "grid_optimization_procedure",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }


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


class TorsionDriveProcedureORM(BaseResultORM):
    """
    A torsion drive  procedure
    """

    __tablename__ = "torsiondrive_procedure"

    id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), primary_key=True)

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
        "polymorphic_identity": "torsiondrive_procedure",
        # to have separate select when querying BaseResultsORM
        "polymorphic_load": "selectin",
    }

    def dict(self):
        d = BaseResultORM.dict(self)

        # Always include optimization history
        d["optimization_history"] = self.optimization_history
        return d


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
