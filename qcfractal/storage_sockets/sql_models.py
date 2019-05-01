import datetime
import dateutil
# from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (Column, Integer, String, Text, DateTime, Boolean,
                        ForeignKey, JSON, Enum, Float, Binary, Table, ARRAY,
                        PrimaryKeyConstraint, inspect)
from sqlalchemy.orm import relationship, object_session, column_property, validates
from qcfractal.interface.models.records import RecordStatusEnum, DriverEnum
from qcfractal.interface.models.task_models import TaskStatusEnum, ManagerStatusEnum, PriorityEnum
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy import event, select, func, and_
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.schema import Index


# Base = declarative_base()


@as_declarative()
class Base:
    """Base declarative class of all ORM models"""

    db_related_fields = ['result_type', 'metadata']

    def to_dict(self, with_id=True, exclude=None):

        tobe_deleted_keys = []

        if not with_id:
            tobe_deleted_keys.append('id')

        if exclude:
            tobe_deleted_keys.extend(exclude)

        dict_obj = [x for x in self.__dict__
                    if not x.startswith('_')
                    and x not in self.db_related_fields
                    and not x.endswith('_obj')
                    and x not in tobe_deleted_keys]

        # add hybrid properties
        for key, prop in inspect(self.__class__).all_orm_descriptors.items():
            if isinstance(prop, hybrid_property):
                dict_obj.append(key)

        ret = {k:getattr(self, k) for k in dict_obj}

        if 'extra' in ret:
            ret.update(ret['extra'])
            del ret['extra']

        return ret

    @classmethod
    def col(cls):
        return cls.__table__.c

    def _update_many_to_many(self, table, parent_id_name, child_id_name,
                            parent_id_val, new_list, old_list=None):
        """Perfomr upsert on a many to many association table
        Does NOT commit changes, parent should optimize when it needs to commit
        raises exception if ids don't exist in the DB
        """

        session = object_session(self)

        old_set = {x for x in old_list} if old_list else set()
        new_set = {x for x in new_list} if new_list else set()


        # Update many-to-many relations
        # Remove old relations and apply the new ones
        if old_set != new_set:
            to_add = new_set - old_set
            to_del = old_set - new_set

            if to_del:
                session.execute(
                    table.delete()
                        .where(and_(table.c[parent_id_name]==parent_id_val,
                                    table.c[child_id_name].in_(to_del)))
                )
            if to_add:
                session.execute(
                    table.insert()\
                        .values([(parent_id_val, my_id) for my_id in to_add])
                )

    def __str__(self):
        if hasattr(self, 'id'):
            return str(self.id)
        return super.__str__(self)

    # @validates('created_on', 'modified_on')
    # def validate_date(self, key, date):
    #     """For SQLite, translate str to dates manulally"""
    #     if date is not None and isinstance(date, str):
    #         date = dateutil.parser.parse(date)
    #     return date


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
    name = Column(String(100), nullable=False)

    tags = Column(JSON)
    tagline = Column(String)
    extra = Column(JSON)  # extra data related to specific collection type

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
    molecular_formula = Column(String)
    molecule_hash = Column(String)

    # Required data
    schema_name = Column(String)
    schema_version = Column(Integer, default=2)
    symbols = Column(JSON)  # Column(ARRAY(String))
    geometry =  Column(JSON)  # Column(ARRAY(Float))

    # Molecule data
    name = Column(String, default="")
    identifiers = Column(JSON)
    comment = Column(String)
    molecular_charge = Column(Float, default=0)
    molecular_multiplicity = Column(Integer, default=1)

    # Atom data
    masses = Column(JSON)  # Column(ARRAY(Float))
    real = Column(JSON)  # Column(ARRAY(Boolean))
    atom_labels = Column(JSON)  # Column(ARRAY(String))
    atomic_numbers = Column(JSON)  # Column(ARRAY(Integer))
    mass_numbers = Column(JSON)  # Column(ARRAY(Integer))

    # Fragment and connection data
    connectivity = Column(JSON)
    fragments = Column(JSON)
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

    # meta = {'indexes': [{'fields': ('hash_index', ), 'unique': True}]}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class BaseResultORM(Base):
    """
        Abstract Base class for ResultORMs and ProcedureORMs
    """

    __tablename__ = 'base_result'

    # for SQL
    result_type = Column(String)  # for inheritance
    task_id = Column(String)  # TODO: not used, for back compatibility

    # Base identification
    id = Column(Integer, primary_key=True)
    hash_index = Column(String) # TODO
    procedure = Column(String(100))  # TODO: may remove
    program = Column(String(100))
    version = Column(Integer)

    # Extra fields
    extras = Column(JSON)
    stdout = Column(Integer, ForeignKey('logs.id'))
    stdout_obj = relationship(LogsORM, lazy='noload', foreign_keys=stdout,
                          cascade="all, delete-orphan", single_parent=True)

    stderr = Column(Integer, ForeignKey('logs.id'))
    stderr_obj = relationship(LogsORM, lazy='noload', foreign_keys=stderr,
                          cascade="all, delete-orphan", single_parent=True)

    error = Column(Integer, ForeignKey('error.id'))
    error_obj = relationship(ErrorORM, lazy='noload', cascade="all, delete-orphan",
                         single_parent=True)

    # Compute status
    # task_id: ObjectId = None  # Removed in SQL
    status = Column(Enum(RecordStatusEnum), nullable=False,
                    default=RecordStatusEnum.incomplete)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # Carry-ons
    provenance = Column(JSON)

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

    id = Column(Integer, ForeignKey('base_result.id', ondelete="CASCADE"), primary_key=True)

    # uniquely identifying a result
    # program = Column(String(100), nullable=False)  # example "rdkit", is it the same as program in keywords?
    driver = Column(String, Enum(DriverEnum), nullable=False)
    method = Column(String(100), nullable=False)  # example "uff"
    basis = Column(String(100))
    molecule = Column(Integer, ForeignKey('molecule.id'))
    molecule_obj = relationship(MoleculeORM, lazy='select')

    # This is a special case where KeywordsORM are denormalized intentionally as they are part of the
    # lookup for a single result and querying a result will not often request the keywords (LazyReference)
    keywords = Column(Integer, ForeignKey('keywords.id'))
    keywords_obj = relationship(KeywordsORM, lazy='select')

    # output related
    return_result = Column(JSON)  # one of 3 types
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


class ProcedureMixin:
    """
        A procedure mixin to be used by specific procedure types
    """

    keywords = Column(JSON)
    qc_spec = Column(JSON)


# ================== Types of ProcedureORMs ================== #

# association table for many to many relation
opt_result_association = Table('opt_result_association', Base.metadata,
    Column('opt_id', Integer, ForeignKey('optimization_procedure.id', ondelete="CASCADE")),
    Column('result_id', Integer, ForeignKey('result.id', ondelete="CASCADE")),
    # PrimaryKeyConstraint('opt_id', 'result_id')
)

class OptimizationProcedureORM(ProcedureMixin, BaseResultORM):
    """
        An Optimization  procedure
    """

    __tablename__ = 'optimization_procedure'

    id = Column(Integer, ForeignKey('base_result.id', ondelete='cascade'),
                      primary_key=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 1)
        self.procedure = "optimization"
        super().__init__(**kwargs)

    schema_version = Column(Integer, default=1)
    initial_molecule = Column(Integer, ForeignKey('molecule.id'))
    initial_molecule_obj = relationship(MoleculeORM, lazy='select',
                                        foreign_keys=initial_molecule)

    # # Results
    energies =  Column(JSON)  #Column(ARRAY(Float))
    final_molecule = Column(Integer, ForeignKey('molecule.id'))
    final_molecule_obj = relationship(MoleculeORM, lazy='select',
                                      foreign_keys=final_molecule)

    # ids, calculated not stored in this table
    # NOTE: this won't work in SQLite since it returns ARRAYS
    trajectory = column_property(
                    select([func.array_agg(opt_result_association.c.result_id)])\
                    .where(opt_result_association.c.opt_id==id)
            )

    # array of objects (results) - Lazy - raise error of accessed
    trajectory_obj = relationship(ResultORM, secondary=opt_result_association,
                                  uselist=True, lazy='noload')


    __mapper_args__ = {
        'polymorphic_identity': 'optimization_procedure',
    }

    __table_args__ = (
        # Index('my_index', "a", "b", unique=True),
    )

    def update_relations(self, trajectory=None, **kwarg):

        # update optimization_results relations
        self._update_many_to_many(opt_result_association, 'opt_id', 'result_id',
                        self.id, trajectory, self.trajectory)

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


# event.listen(OptimizationProcedureORM, 'before_insert', add_relations)

# association table for many to many relation
# torsion_opt_association = Table('torsion_opt_association', Base.metadata,
#     Column('torsion_id', Integer, ForeignKey('torsiondrive_procedure.id', ondelete="CASCADE")),
#     Column('opt_id', Integer, ForeignKey('optimization_procedure.id', ondelete="CASCADE")),
#     Column('key', String),
#     Index('torsion_id', 'key', unique=True)
# )

class OptimizationHistory(Base):
    """Association table for many to many"""

    __tablename__ = 'optimization_history'

    torsion_id = Column(Integer, ForeignKey('torsiondrive_procedure.id', ondelete='cascade'), primary_key=True)
    opt_id = Column(Integer, ForeignKey('optimization_procedure.id', ondelete='cascade'), primary_key=True)
    key = Column(String, nullable=False, primary_key=True)
    # Index('torsion_id', 'key', unique=True)

    optimization_obj = relationship(OptimizationProcedureORM, lazy="joined")


# association table for many to many relation
torsion_init_mol_association = Table('torsion_init_mol_association', Base.metadata,
    Column('torsion_id', Integer, ForeignKey('torsiondrive_procedure.id', ondelete="CASCADE")),
    Column('molecule_id', Integer, ForeignKey('molecule.id', ondelete="CASCADE"))
)

class TorsionDriveProcedureORM(ProcedureMixin, BaseResultORM):
    """
        A torsion drive  procedure
    """

    __tablename__ = 'torsiondrive_procedure'

    id = Column(Integer, ForeignKey('base_result.id', ondelete='cascade'),
                      primary_key=True)

    def __init__(self, **kwargs):
        kwargs.setdefault("version", 1)
        self.procedure = "torsiondrive"
        self.program = "torsiondrive"
        super().__init__(**kwargs)

    # input data (along with the mixin)

    # ids of the many to many relation
    initial_molecule = column_property(
                    select([func.array_agg(torsion_init_mol_association.c.molecule_id)])\
                    .where(torsion_init_mol_association.c.torsion_id==id)
            )
    # actual objects relation M2M, never loaded here
    initial_molecule_obj = relationship(MoleculeORM,
                                        secondary=torsion_init_mol_association,
                                        uselist=True, lazy='noload')


    keywords = Column(JSON)  # TODO: same as BaseRecord!!!
    optimization_spec = Column(JSON)

    # Output data
    final_energy_dict = Column(JSON)
    minimum_positions = Column(JSON)

    # # ids of the many to many relation
    # optimization_history = column_property(
    #                 select([func.array_agg(torsion_opt_association.c.opt_id)])\
    #                 .where(torsion_opt_association.c.torsion_id==id)
    #         )
    # # actual objects relation M2M, never loaded here
    # optimization_history_obj = relationship(OptimizationProcedureORM,
    #                                     secondary=torsion_opt_association,
    #                                     uselist=True, lazy='noload')


    optimization_history_obj = relationship(OptimizationHistory,
        cascade="all, delete-orphan", backref="torsiondrive_procedure"
    )

    @hybrid_property
    def optimization_history(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation in the form of a dict"""

        ret = {}
        try:
            for opt_history in self.optimization_history_obj:
                if opt_history.key in ret:
                    ret[opt_history.key].append(opt_history.opt_id)
                else:
                    ret[opt_history.key] = [opt_history.opt_id]

        except Exception as err:
            # raises exception of first access!!
            print(err)

        return ret

    @optimization_history.setter
    def optimization_history(self, dict_values):
        """A private copy of the opt history as a dict
        Key: list of optimization procedures"""

        return dict_values

    __mapper_args__ = {
        'polymorphic_identity': 'torsiondrive_procedure',
    }

    def update_relations(self, initial_molecule=None, optimization_history=None, **kwarg):

        # update torsion molecule relation
        self._update_many_to_many(torsion_init_mol_association, 'torsion_id', 'molecule_id',
                        self.id, initial_molecule, self.initial_molecule)

        # # update torsion optimization procedure relation
        # self._update_many_to_many(torsion_opt_association, 'torsion_id', 'opt_id',
        #                 self.id, optimization_history, self.optimization_history)

        session = object_session(self)
        self.optimization_history_obj = []
        for key in optimization_history:
            for opt_id in optimization_history[key]:
                opt_history = OptimizationHistory(torsion_id=id, opt_id=opt_id, key=key)
                self.optimization_history_obj.append(opt_history)

        # No need for the following because the session is committed with parent save
        # session.add_all(self.optimization_history_obj)
        # session.add(self)
        # session.commit()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# class Spec(db.DynamicEmbeddedDocument):
#     """ The spec of a task in the queue
#         This is an embedded document, meaning that it will be embedded
#         in the task_queue collection and won't be stored as a seperate
#         collection/table --> for faster parsing
#     """
#
#     function = Column(String)
#     args = Column(JSON)  # fast, can take any structure
#     kwargs = Column(JSON)


class TaskQueueORM(Base):
    """A queue of tasks corresponding to a procedure

       Notes: don't sort query results without having the index sorted
              will impact the performce
    """

    __tablename__ = "task_queue"


    id = Column(Integer, primary_key=True)

    spec = Column(JSON)

    # others
    tag = Column(String, default=None)
    parser = Column(String, default='')
    program = Column(String)
    procedure = Column(String)
    status = Column(Enum(TaskStatusEnum), default=TaskStatusEnum.waiting)
    priority = Column(Enum(PriorityEnum), default=PriorityEnum.NORMAL)
    manager = Column(String, default=None)
    error = Column(String)  # TODO: is this an error object? should be in results?

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # can reference ResultORMs or any ProcedureORM
    base_result = Column(Integer, ForeignKey("base_result.id"), unique=True)
    base_result_obj = relationship(BaseResultORM, lazy='select')  # or lazy='joined'

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

    procedure_id = Column(Integer, ForeignKey("base_result.id"), unique=True)
    procedure_obj = relationship(BaseResultORM, lazy='joined')

    extra = Column(JSON)

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
    permissions = Column(JSON)  # Column(ARRAY(String))

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

    status = Column(Enum(ManagerStatusEnum), default=ManagerStatusEnum.inactive)

    created_on = Column(DateTime, default=datetime.datetime.utcnow)
    modified_on = Column(DateTime, default=datetime.datetime.utcnow)

    # meta = {'collection': 'queue_manager', 'indexes': ['status', 'name', 'modified_on']}

    # def save(self, *args, **kwargs):
    #     """Override save to update modified_on"""
    #     self.modified_on = datetime.datetime.utcnow()
    #     if not self.created_on:
    #         self.created_on = datetime.datetime.utcnow()
    #
    #     return super(QueueManagerORM, self).save(*args, **kwargs)
