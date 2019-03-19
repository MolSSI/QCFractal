import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Integer, String, Text, DateTime,
                        ForeignKey, Binary, ARRAY, JSON, Enum, Float)
from sqlalchemy.orm import relationship
# from sqlalchemy_utils.types.choice import ChoiceType
from qcfractal.interface.models.records import RecordStatusEnum, DriverEnum
from qcfractal.interface.models.task_models import TaskStatusEnum, ManagerStatusEnum

# pip install sqlalchemy psycopg2 sqlalchemy_utils

Base = declarative_base()

class AccessLogORM(Base):
    __tablename__ = 'access_log'

    id = Column(Integer, primary_key=True)
    ip_address = Column(String)
    date = Column(DateTime, default=datetime.datetime.utcnow)
    type = Column(String)


class LogsORM(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)
    value = Column(Text, nullable=False)


class ErrorORM(Base):
    __tablename__ = "error"

    id = Column(Integer, primary_key=True)
    value = Column(Text, nullable=False)


class CollectionORM(Base):
    """
        A collection of precomuted workflows such as datasets, ..

        This is a dynamic document, so it will accept any number of
        extra fields (expandable and uncontrolled schema)
    """

    __tablename__ = "collection"

    id = Column(Integer, primary_key=True)

    collection = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)  # Example 'water'

    # meta = {
    #     'indexes': [{
    #         'fields': ('collection', 'name'),
    #         'unique': True
    #     }]
    # }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class MoleculeORM(Base):
    """
        The molecule DB collection is managed by pymongo, so far
    """

    __tablename__ = "molecule"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    symbols = Column(ARRAY(String))
    molecular_formula = Column(String)
    molecule_hash = Column(String)
    geometry = Column(ARRAY(String))


    # def save(self, *args, **kwargs):
    #     """Override save to add molecule_hash"""
    #     # self.molecule_hash = self.create_hash()
    #
    #     return super(MoleculeORM, self).save(*args, **kwargs)

    def __str__(self):
        return str(self.id)

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
    values = Column(Binary)

    # meta = {'indexes': [{'fields': ('hash_index', ), 'unique': True}]}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class BaseResultORM(Base):
    """
        Abstract Base class for ResultORMs and ProcedureORMs
    """

    __tablename__ = 'base_result'

    # for SQL
    result_type = Column(Integer)  # for inheritance
    parent_id = Column(Integer, ForeignKey('base_result.id'))

    # Base identification
    id = Column(Integer, primary_key=True)
    # hash_index = Column(String) # TODO
    procedure = Column(String(100))  # TODO: may remove
    program = Column(String(100))
    version = Column(Integer)

    # Extra fields
    extras = Column(Binary)
    stdout_id = Column(Integer, ForeignKey('logs.id'))
    stdout = relationship(LogsORM, lazy=True, foreign_keys=stdout_id,
                          cascade="all, delete-orphan", single_parent=True)

    stderr_id = Column(Integer, ForeignKey('logs.id'))
    stderr = relationship(LogsORM, lazy=True, foreign_keys=stderr_id,
                          cascade="all, delete-orphan", single_parent=True)

    error_id = Column(Integer, ForeignKey('error.id'))
    error = relationship(ErrorORM, lazy=True, cascade="all, delete-orphan",
                         single_parent=True)

    # Compute status
    # task_id: ObjectId = None  # Removed in SQL
    status = Column(Enum(RecordStatusEnum), nullable=False,
                    default=RecordStatusEnum.incomplete)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # Carry-ons
    provenance = Column(Binary)

    # meta = {
    #     # 'allow_inheritance': True,
    #     'indexes': ['status']
    # }

    # def save(self, *args, **kwargs):
    #     """Override save to set defaults"""
    #
    #     self.modified_on = datetime.datetime.utcnow()
    #     if not self.created_on:
    #         self.created_on = datetime.datetime.utcnow()
    #
    #     return super(BaseResultORM, self).save(*args, **kwargs)

    __mapper_args__ = {
        'polymorphic_on': 'result_type'
    }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ResultORM(BaseResultORM):
    """
        Hold the result of an atomic single calculation
    """

    __tablename__ = "result"

    id = Column(Integer, ForeignKey('base_result.id'), primary_key=True)

    # uniquely identifying a result
    # program = Column(String(100), nullable=False)  # example "rdkit", is it the same as program in keywords?
    driver = Column(String, Enum(DriverEnum), nullable=False)
    method = Column(String(100), nullable=False)  # example "uff"
    basis = Column(String(100))
    molecule_id = Column(Integer, ForeignKey('molecule.id'))
    molecule = relationship("MoleculeORM", lazy=True)

    # This is a special case where KeywordsORM are denormalized intentionally as they are part of the
    # lookup for a single result and querying a result will not often request the keywords (LazyReference)
    keywords_id = Column(Integer, ForeignKey('keywords.id'))
    keywords = relationship("KeywordsORM")

    # output related
    return_result = Column(Binary)  # one of 3 types
    properties = Column(JSON)  # TODO: may use JSONB in the future


    # TODO: Do they still exist?
    # schema_name = Column(String)  # default="qc_ret_data_output"??
    # schema_version = Column(Integer)

    # meta = {
    #     # 'collection': 'result',
    #     'indexes': [
    #         {
    #             'fields': ('program', 'driver', 'method', 'basis', 'molecule', 'keywords'),
    #             'unique': True
    #         },
    #     ]
    # }

    __mapper_args__ = {
        'polymorphic_identity': 'result',
    }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# class ProcedureORM(Base):
#     """
#         A procedure is a group of related results applied to a list of molecules
#     """
#
#     __abstract__ = True
#
#     id = Column(Integer, ForeignKey('base_result.id'), primary_key=True)
#
#     __mapper_args__ = {
#         # 'polymorphic_on': 'procedure_type'
#     }


# ================== Types of ProcedureORMs ================== #


class OptimizationProcedureORM(BaseResultORM):
    """
        An Optimization  procedure
    """

    __tablename__ = 'optimization_procedure'

    id = Column(Integer, ForeignKey('base_result.id'), primary_key=True)

    # Version data
    version = Column(Integer, default=1)
    # procedure = Column(String(100), default="optimization")
    schema_version = Column(Integer, default=1)

    # Input data
    initial_molecule_id = Column(Integer, ForeignKey('molecule.id'))
    initial_molecule = relationship("MoleculeORM", lazy=True,
                                    foreign_keys=[initial_molecule_id])

    qc_spec = Column(Binary)

    # Results
    energies = Column(ARRAY(Float))
    final_molecule_id = Column(Integer, ForeignKey('molecule.id'))
    final_molecule = relationship("MoleculeORM", lazy=True,
                                  foreign_keys=[final_molecule_id])

    # # array of objects (results)
    # trajectory = relationship("BaseResultORM", lazy=False,
    #                           foreign_keys="[parent_id]")

    __mapper_args__ = {
        'polymorphic_identity': 'optimization_procedure',
    }


class TorsiondriveProcedureORM(BaseResultORM):
    """
        An torsion drive  procedure
    """

    __tablename__ = 'torsiondrive_procedure'

    id = Column(Integer, ForeignKey('base_result.id'), primary_key=True)

    # TODO: add more fields

    __mapper_args__ = {
        'polymorphic_identity': 'torsiondrive_procedure',
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# class Spec(db.DynamicEmbeddedDocument):
#     """ The spec of a task in the queue
#         This is an embedded document, meaning that it will be embedded
#         in the task_queue collection and won't be stored as a seperate
#         collection/table --> for faster parsing
#     """
#
#     function = Column(String)
#     args = Column(Binary)  # fast, can take any structure
#     kwargs = Column(Binary)


class TaskQueueORM(Base):
    """A queue of tasks corresponding to a procedure

       Notes: don't sort query results without having the index sorted
              will impact the performce
    """

    __tablename__ = "task_queue"

    id = Column(Integer, primary_key=True)

    spec = Column(Binary)

    # others
    tag = Column(String, default=None)
    parser = Column(String, default='')
    status = Column(Enum(TaskStatusEnum), default=TaskStatusEnum.waiting)
    manager = Column(String, default=None)

    created_on = Column(DateTime, nullable=False)
    modified_on = Column(DateTime, nullable=False)

    # can reference ResultORMs or any ProcedureORM
    base_result_id = Column(Integer, ForeignKey("base_result.id"))  # todo:
    base_result = relationship("BaseResultORM")

    # meta = {
    #     'indexes': [
    #         'created_on',
    #         'status',
    #         'manager',
    #         {
    #             'fields': ('base_result', ),
    #             'unique': True
    #         },  # new
    #     ]
    # }

    # def save(self, *args, **kwargs):
    #     """Override save to update modified_on"""
    #     self.modified_on = datetime.datetime.utcnow()
    #     if not self.created_on:
    #         self.created_on = datetime.datetime.utcnow()
    #
    #     return super(TaskQueueORM, self).save(*args, **kwargs)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ServiceQueueORM(Base):

    __tablename__ = "service_queue"

    id = Column(Integer, primary_key=True)

    status = Column(Enum(TaskStatusEnum), default=TaskStatusEnum.waiting)
    tag = Column(String, default=None)
    hash_index = Column(String, nullable=False)

    procedure_id = Column(Integer, ForeignKey("base_result.id"))
    procedure = relationship("BaseResultORM")

    # created_on = Column(DateTime, nullable=False)
    # modified_on = Column(DateTime, nullable=False)

    # meta = {
    #     'indexes': [
    #         'status',
    #         {
    #             'fields': ("status", "tag", "hash_index"),
    #             'unique': False
    #         },
    #         # {'fields': ('procedure',), 'unique': True}
    #     ]
    # }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class UserORM(Base):

    __tablename__ = "user"

    id = Column(Integer, primary_key=True)

    username = Column(String, nullable=False, unique=True)
    password = Column(Binary, nullable=False)
    permissions = Column(ARRAY(String))

    # meta = {'collection': 'user', 'indexes': ['username']}


class QueueManagerORM(Base):
    """
    """

    __tablename__ = "queue_manager"

    id = Column(Integer, primary_key=True)

    name = Column(String, unique=True)
    cluster = Column(String)
    hostname = Column(String)
    uuid = Column(String)
    tag = Column(String)

    # counts
    completed = Column(Integer, default=0)
    submitted = Column(Integer, default=0)
    failures = Column(Integer, default=0)
    returned = Column(Integer, default=0)

    status = Column(Enum(ManagerStatusEnum),
                    default=ManagerStatusEnum.inactive)

    created_on = Column(DateTime, nullable=False)
    modified_on = Column(DateTime, nullable=False)

    # meta = {'collection': 'queue_manager', 'indexes': ['status', 'name', 'modified_on']}

    # def save(self, *args, **kwargs):
    #     """Override save to update modified_on"""
    #     self.modified_on = datetime.datetime.utcnow()
    #     if not self.created_on:
    #         self.created_on = datetime.datetime.utcnow()
    #
    #     return super(QueueManagerORM, self).save(*args, **kwargs)
