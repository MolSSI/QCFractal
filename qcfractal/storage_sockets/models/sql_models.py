"""
Basic ORM models of the QCFractal database

Note: avoid circular import here by including the name of the class
in relations and foreign keys are a string (see TaskQueueORM.base_result_obj)
"""

import datetime

# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
)
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.sql import text

from qcfractal.interface.models.task_models import ManagerStatusEnum, PriorityEnum, TaskStatusEnum
from qcfractal.interface.models.common_models import CompressionEnum
from qcfractal.storage_sockets.models.sql_base import Base, MsgpackExt


class AccessLogORM(Base):
    __tablename__ = "access_log"

    id = Column(Integer, primary_key=True)
    access_date = Column(DateTime, default=datetime.datetime.utcnow)
    access_method = Column(String, nullable=False)
    access_type = Column(String, nullable=False)

    # Note: no performance difference between varchar and text in postgres
    # will mostly have a serialized JSON, but not stored as JSON for speed
    extra_params = Column(String)

    # user info
    ip_address = Column(String)
    user_agent = Column(String)

    # extra computed geo data
    city = Column(String)
    country = Column(String)
    country_code = Column(String)
    ip_lat = Column(String)
    ip_long = Column(String)
    postal_code = Column(String)
    subdivision = Column(String)

    __table_args__ = (Index("access_type", "access_date"),)


class ServerStatsLogORM(Base):
    __tablename__ = "server_stats_log"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    # Raw counts
    collection_count = Column(Integer)
    molecule_count = Column(Integer)
    result_count = Column(Integer)
    kvstore_count = Column(Integer)
    access_count = Column(Integer)

    # States
    result_states = Column(JSON)

    # Database
    db_total_size = Column(BigInteger)
    db_table_size = Column(BigInteger)
    db_index_size = Column(BigInteger)
    db_table_information = Column(JSON)

    __table_args__ = (Index("ix_server_stats_log_timestamp", "timestamp"),)


class VersionsORM(Base):
    __tablename__ = "versions"

    id = Column(Integer, primary_key=True)
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    elemental_version = Column(String, nullable=False)
    fractal_version = Column(String, nullable=False)
    engine_version = Column(String)


class KVStoreORM(Base):
    """TODO: rename to """

    __tablename__ = "kv_store"

    id = Column(Integer, primary_key=True)
    compression = Column(Enum(CompressionEnum), nullable=True)
    compression_level = Column(Integer, nullable=True)
    value = Column(JSON, nullable=True)
    data = Column(LargeBinary, nullable=True)


class MoleculeORM(Base):
    """
    The molecule DB collection is managed by pymongo, so far
    """

    __tablename__ = "molecule"

    id = Column(Integer, primary_key=True)
    molecular_formula = Column(String)
    molecule_hash = Column(String)

    # Required data
    schema_name = Column(String)
    schema_version = Column(Integer, default=2)
    symbols = Column(MsgpackExt, nullable=False)
    geometry = Column(MsgpackExt, nullable=False)

    # Molecule data
    name = Column(String, default="")
    identifiers = Column(JSON)
    comment = Column(String)
    molecular_charge = Column(Float, default=0)
    molecular_multiplicity = Column(Integer, default=1)

    # Atom data
    masses = Column(MsgpackExt)
    real = Column(MsgpackExt)
    atom_labels = Column(MsgpackExt)
    atomic_numbers = Column(MsgpackExt)
    mass_numbers = Column(MsgpackExt)

    # Fragment and connection data
    connectivity = Column(JSON)
    fragments = Column(MsgpackExt)
    fragment_charges = Column(JSON)  # Column(ARRAY(Float))
    fragment_multiplicities = Column(JSON)  # Column(ARRAY(Integer))

    # Orientation
    fix_com = Column(Boolean, default=False)
    fix_orientation = Column(Boolean, default=False)
    fix_symmetry = Column(String)

    # Extra
    provenance = Column(JSON)
    extras = Column(JSON)

    # def __str__(self):
    #     return str(self.id)

    __table_args__ = (
        Index("ix_molecule_hash", "molecule_hash", unique=False),  # dafault index is B-tree
        # TODO: no index on molecule_formula
    )

    # meta = {
    #
    #     'indexes': [
    #         {
    #             'fields': ('molecule_hash', ),
    #             'unique': False
    #         },  # should almost be unique
    #         {
    #             'fields': ('molecular_formula', ),
    #             'unique': False
    #         }
    #     ]
    # }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class KeywordsORM(Base):
    """
    KeywordsORM are unique for a specific program and name
    """

    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    hash_index = Column(String, nullable=False)
    values = Column(JSON)

    lowercase = Column(Boolean, default=True)
    exact_floats = Column(Boolean, default=False)
    comments = Column(String)

    __table_args__ = (Index("ix_keywords_hash_index", "hash_index", unique=True),)
    # meta = {'indexes': [{'fields': ('hash_index', ), 'unique': True}]}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class TaskQueueORM(Base):
    """A queue of tasks corresponding to a procedure

    Notes: don't sort query results without having the index sorted
           will impact the performance
    """

    __tablename__ = "task_queue"

    id = Column(Integer, primary_key=True)

    spec = Column(MsgpackExt, nullable=False)

    # others
    tag = Column(String, default=None)
    parser = Column(String, default="")
    program = Column(String)
    procedure = Column(String)
    status = Column(Enum(TaskStatusEnum), default=TaskStatusEnum.waiting)
    priority = Column(Integer, default=int(PriorityEnum.NORMAL))
    manager = Column(String, ForeignKey("queue_manager.name", ondelete="SET NULL"), default=None)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # TODO: for back-compatibility with mongo, tobe removed
    # (requries modifying pydantic model)
    @hybrid_property
    def base_result(self):
        return self.base_result_id

    @base_result.setter
    def base_result(self, val):
        self.base_result_id = int(val)

    # can reference ResultORMs or any ProcedureORM
    base_result_id = Column(Integer, ForeignKey("base_result.id", ondelete="cascade"), unique=True)
    base_result_obj = relationship("BaseResultORM", lazy="select")  # or lazy='joined'

    # An important special case is ORDER BY in combination with LIMIT n: an
    # explicit sort will have to process all the data to identify the first n
    # rows, but if there is an index matching the ORDER BY, the first n rows
    # can be retrieved directly, without scanning the remainder at all.
    __table_args__ = (
        Index("ix_task_queue_created_on", "created_on"),
        Index("ix_task_queue_keys", "status", "program", "procedure", "tag"),
        Index("ix_task_queue_manager", "manager"),
        Index("ix_task_queue_base_result_id", "base_result_id"),
        Index("ix_task_waiting_sort", text("priority desc,  created_on")),
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ServiceQueueORM(Base):

    __tablename__ = "service_queue"

    id = Column(Integer, primary_key=True)

    status = Column(Enum(TaskStatusEnum), default=TaskStatusEnum.waiting)
    tag = Column(String, default=None)
    hash_index = Column(String, nullable=False)

    procedure_id = Column(Integer, ForeignKey("base_result.id"), unique=True)
    procedure_obj = relationship("BaseResultORM", lazy="joined")

    priority = Column(Integer, default=int(PriorityEnum.NORMAL))
    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    extra = Column(MsgpackExt)

    __table_args__ = (
        Index("ix_service_queue_status", "status"),
        Index("ix_service_queue_priority", "priority"),
        Index("ix_service_queue_modified_on", "modified_on"),
        Index("ix_service_queue_status_tag_hash", "status", "tag"),
        Index("ix_service_queue_hash_index", "hash_index"),
    )


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class UserORM(Base):

    __tablename__ = "user"

    id = Column(Integer, primary_key=True)

    username = Column(String, nullable=False, unique=True)  # indexed and unique
    password = Column(LargeBinary, nullable=False)
    permissions = Column(JSON)  # Column(ARRAY(String))


class QueueManagerLogORM(Base):

    __tablename__ = "queue_manager_logs"

    id = Column(Integer, primary_key=True)
    manager_id = Column(Integer, ForeignKey("queue_manager.id"), nullable=False)

    timestamp = Column(DateTime, default=datetime.datetime.utcnow, nullable=False)

    completed = Column(Integer, nullable=True)
    submitted = Column(Integer, nullable=True)
    failures = Column(Integer, nullable=True)

    total_worker_walltime = Column(Float, nullable=True)
    total_task_walltime = Column(Float, nullable=True)
    active_tasks = Column(Integer, nullable=True)
    active_cores = Column(Integer, nullable=True)
    active_memory = Column(Float, nullable=True)

    __table_args__ = (Index("ix_queue_manager_log_timestamp", "timestamp"),)


class QueueManagerORM(Base):
    """"""

    __tablename__ = "queue_manager"

    id = Column(Integer, primary_key=True)

    name = Column(String, unique=True)
    cluster = Column(String)
    hostname = Column(String)
    username = Column(String)
    uuid = Column(String)
    tag = Column(String)

    # Count at current time
    completed = Column(Integer, default=0)
    submitted = Column(Integer, default=0)
    failures = Column(Integer, default=0)
    returned = Column(Integer, default=0)

    total_worker_walltime = Column(Float, nullable=True)
    total_task_walltime = Column(Float, nullable=True)
    active_tasks = Column(Integer, nullable=True)
    active_cores = Column(Integer, nullable=True)
    active_memory = Column(Float, nullable=True)

    # Adapter Information
    configuration = Column(JSON, nullable=True)

    status = Column(Enum(ManagerStatusEnum), default=ManagerStatusEnum.inactive)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    qcengine_version = Column(String)
    manager_version = Column(String)
    programs = Column(JSON)
    procedures = Column(JSON)

    __table_args__ = (Index("ix_queue_manager_status", "status"), Index("ix_queue_manager_modified_on", "modified_on"))
