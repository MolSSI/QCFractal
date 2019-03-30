import datetime
import json
from collections.abc import Iterable

import bson
import mongoengine as db


class CustomDynamicDocument(db.DynamicDocument):
    """
    This class is serializable into standard json.
    """

    def to_json_obj(self, with_id=True):
        """Removes object types like $date, _cls, and $oid
            Also, replaces _id with id
            Assumes one level of ReferenceFields which is suffice
        """

        data = json.loads(bson.json_util.dumps(self.to_mongo()))

        for key, value in data.items():
            if isinstance(value, dict) and len(value) == 1:
                (subkey, subvalue), = value.items()
                if subkey.startswith('$'):
                    data[key] = subvalue
            elif isinstance(value, Iterable) and '_ref' in value:
                data[key] = data[key]['_ref']
                data[key]['ref'] = data[key]['$ref']
                del data[key]['$ref']
                data[key]['id'] = str(data[key]['$id']['$oid'])
                del data[key]['$id']

        if with_id:
            data['id'] = data['_id']
        del data['_id']

        data.pop("_cls", None)

        return data

    meta = {
        'abstract': True,
    }


class KVStoreORM(CustomDynamicDocument):

    value = db.DynamicField(required=True)
    meta = {
        'collection': 'kv_store',
    }


class CollectionORM(CustomDynamicDocument):
    """
        A collection of precomputed workflows such as datasets, ...

        This is a dynamic document, so it will accept any number of
        extra fields (expandable and uncontrolled schema).
    """

    collection = db.StringField(required=True)  # , choices=['dataset', '?'])
    lname = db.StringField(required=True)  # Example 'water'

    meta = {
        'collection': 'collection',  # DB collection/table name
        'indexes': [{
            'fields': ('collection', 'lname'),
            'unique': True
        }]
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class MoleculeORM(CustomDynamicDocument):
    """
        The molecule DB collection is managed by pymongo, so far.
    """

    name = db.StringField()
    symbols = db.ListField()
    molecular_formula = db.StringField()
    molecule_hash = db.StringField()
    geometry = db.ListField()

    def create_hash(self):
        """ TODO: create a special hash before saving"""
        return ''

    def save(self, *args, **kwargs):
        """Override save to add molecule_hash"""
        # self.molecule_hash = self.create_hash()

        return super(MoleculeORM, self).save(*args, **kwargs)

    def __str__(self):
        return str(self.id)

    meta = {
        'collection':
        'molecule',
        'indexes': [
            {
                'fields': ('molecule_hash', ),
                'unique': False
            },  # should almost be unique
            {
                'fields': ('molecular_formula', ),
                'unique': False
            }
        ]
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class KeywordsORM(CustomDynamicDocument):
    """
        KeywordsORM are unique for a specific program and name.
    """

    # TODO: pull choices from const config
    hash_index = db.StringField(required=True)
    values = db.DynamicField()

    meta = {'indexes': [{'fields': ('hash_index', ), 'unique': True}]}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class BaseResultORM(CustomDynamicDocument):
    """
        Abstract Base class for ResultORMs and ProcedureORMs.
    """

    # queue related
    task_id = db.StringField()  # ObjectId, reference task_queue but without validation
    status = db.StringField(required=True, choices=['COMPLETE', 'INCOMPLETE', 'ERROR'])

    created_on = db.DateTimeField(required=True)
    modified_on = db.DateTimeField(required=True)

    meta = {
        'abstract': True,
        # 'allow_inheritance': True,
        'indexes': ['status']
    }

    def save(self, *args, **kwargs):
        """Override save to set defaults"""

        self.modified_on = datetime.datetime.utcnow()
        if not self.created_on:
            self.created_on = datetime.datetime.utcnow()

        return super(BaseResultORM, self).save(*args, **kwargs)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ResultORM(BaseResultORM):
    """
        Hold the result of an atomic single calculation.
    """

    # uniquely identifying a result
    program = db.StringField(required=True)  # example "rdkit", is it the same as program in keywords?
    driver = db.StringField(required=True)  # example "gradient"
    method = db.StringField(required=True)  # example "uff"
    basis = db.StringField()
    molecule = db.LazyReferenceField(MoleculeORM, required=True)

    # This is a special case where KeywordsORM are denormalized intentionally as they are part of the
    # lookup for a single result and querying a result will not often request the keywords (LazyReference)
    keywords = db.LazyReferenceField(KeywordsORM)

    # output related
    properties = db.DynamicField()  # accept any, no validation
    return_result = db.DynamicField()  # better performance than db.ListField(db.FloatField())
    provenance = db.DynamicField()  # or an Embedded Documents with a structure?

    schema_name = db.StringField()  # default="qc_ret_data_output"??
    schema_version = db.IntField()  # or String?

    meta = {
        'collection': 'result',
        'indexes': [
            {
                'fields': ('program', 'driver', 'method', 'basis', 'molecule', 'keywords'),
                'unique': True
            },
        ]
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ProcedureORM(BaseResultORM):
    """
        A procedure is a group of related results applied to a list of molecules.
    """

    procedure = db.StringField(required=True)
    program = db.StringField(required=True)  # example: 'Geometric'
    hash_index = db.StringField(required=True)

    # Unlike ResultORMs KeywordsORM are not denormalized here as a ProcedureORM query will always want the
    # keywords and the keywords are not part of the index.
    keywords = db.DynamicField()

    meta = {
        'collection':
        'procedure',
        'allow_inheritance':
        True,
        'indexes': [
            # TODO: needs a unique index, + molecule?
            {
                'fields': ('procedure', 'program'),
                'unique': False
            },  # TODO: check
            {
                'fields': ('hash_index', ),
                'unique': False
            }  # used in queries
        ]
    }


# ================== Types of ProcedureORMs ================== #


class OptimizationProcedureORM(ProcedureORM):
    """
        An Optimization procedure.
    """

    procedure = db.StringField(default='optimization', required=True)

    initial_molecule = db.LazyReferenceField(MoleculeORM)
    final_molecule = db.LazyReferenceField(MoleculeORM)


class TorsiondriveProcedureORM(ProcedureORM):
    """
        An torsion drive procedure.
    """

    procedure = db.StringField(default='torsiondrive', required=True)

    # TODO: add more fields

    meta = {'indexes': []}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class TaskQueueORM(CustomDynamicDocument):
    """A queue of tasks corresponding to a procedure.

       Notes: Don't sort query results without having the index sorted
              or it will impact the performance.
    """

    spec = db.DynamicField()
    parser = db.StringField()
    status = db.StringField(default='WAITING', choices=['RUNNING', 'WAITING', 'ERROR', 'COMPLETE'])

    program = db.StringField()
    procedure = db.StringField()
    manager = db.StringField()

    # others
    priority: db.IntField(default=1)
    tag = db.StringField(default=None)

    # can reference ResultORMs or any ProcedureORM
    base_result = db.GenericLazyReferenceField(dbref=True)  # use res.id and res.document_type (class)

    created_on = db.DateTimeField(required=True)
    modified_on = db.DateTimeField(required=True)


    meta = {
        'collection': 'task_queue',
        'indexes': [
            'created_on',
            'status',

            # Specification fields
            {
                'fields': ('program', 'procedure'),
            },  # new
            'manager',

            # order effects
            'tag',
            'priority',

            {
                'fields': ('base_result', ),
                'unique': True
            },  # new
        ]
    }

    def save(self, *args, **kwargs):
        """Override save to update modified_on."""
        self.modified_on = datetime.datetime.utcnow()
        if not self.created_on:
            self.created_on = datetime.datetime.utcnow()

        return super(TaskQueueORM, self).save(*args, **kwargs)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ServiceQueueORM(CustomDynamicDocument):

    status = db.StringField(default='WAITING', choices=['RUNNING', 'WAITING', 'ERROR', 'COMPLETE'])
    tag = db.StringField(default=None)
    hash_index = db.StringField(required=True)
    procedure_id = db.LazyReferenceField(ProcedureORM)

    # created_on = db.DateTimeField(required=True)
    # modified_on = db.DateTimeField(required=True)

    meta = {
        'collection':
        'service_queue',
        'indexes': [
            'status',
            {
                'fields': ("status", "tag", "hash_index"),
                'unique': False
            },
            # {'fields': ('procedure',), 'unique': True}
        ]
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class UserORM(CustomDynamicDocument):

    username = db.StringField(required=True, unique=True)
    password = db.BinaryField(required=True)
    permissions = db.ListField()

    meta = {'collection': 'user', 'indexes': ['username']}


class QueueManagerORM(CustomDynamicDocument):
    """
    """

    name = db.StringField(unique=True)
    cluster = db.StringField()
    hostname = db.StringField()
    uuid = db.StringField()

    username = db.StringField()
    qcengine_version = db.StringField()
    manager_version = db.StringField()

    tag = db.StringField()
    programs = db.DynamicField()
    procedures = db.DynamicField()

    # counts
    completed = db.IntField(default=0)
    submitted = db.IntField(default=0)
    failures = db.IntField(default=0)
    returned = db.IntField(default=0)

    status = db.StringField(default='INACTIVE', choices=['ACTIVE', 'INACTIVE'])

    created_on = db.DateTimeField(required=True)
    modified_on = db.DateTimeField(required=True)

    meta = {'collection': 'queue_manager', 'indexes': ['status', 'name', 'modified_on']}

    def save(self, *args, **kwargs):
        """Override save to update modified_on."""
        self.modified_on = datetime.datetime.utcnow()
        if not self.created_on:
            self.created_on = datetime.datetime.utcnow()

        return super(QueueManagerORM, self).save(*args, **kwargs)
