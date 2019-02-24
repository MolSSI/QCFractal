import datetime
import json
from collections.abc import Iterable

import bson
import mongoengine as db


class CustomDynamicDocument(db.DynamicDocument):
    """
    This class is serializable into standard json
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

        return data

    meta = {
        'abstract': True,
    }


class Collection(CustomDynamicDocument):
    """
        A collection of precomuted workflows such as datasets, ..

        This is a dynamic document, so it will accept any number of
        extra fields (expandable and uncontrolled schema)
    """

    collection = db.StringField(required=True)  # , choices=['dataset', '?'])
    name = db.StringField(required=True)  # Example 'water'

    meta = {
        'collection': 'collection',  # DB collection/table name
        'indexes': [{
            'fields': ('collection', 'name'),
            'unique': True
        }]
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Molecule(CustomDynamicDocument):
    """
        The molecule DB collection is managed by pymongo, so far
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

        return super(Molecule, self).save(*args, **kwargs)

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


class Keywords(CustomDynamicDocument):
    """
        Keywords are unique for a specific program and name
    """

    # TODO: pull choices from const config
    hash_index = db.StringField(required=True)
    values = db.DynamicField()

    meta = {'indexes': [{'fields': ('hash_index', ), 'unique': True}]}

    def __str__(self):
        return str(self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class BaseResult(CustomDynamicDocument):
    """
        Abstract Base class for Results and Procedures
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

        if not self.status:
            self.status = 'INCOMPLETE'

        self.modified_on = datetime.datetime.utcnow()
        if not self.created_on:
            self.created_on = datetime.datetime.utcnow()

        return super(BaseResult, self).save(*args, **kwargs)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Result(BaseResult):
    """
        Hold the result of an atomic single calculation
    """

    # uniquely identifying a result
    program = db.StringField(required=True)  # example "rdkit", is it the same as program in keywords?
    driver = db.StringField(required=True)  # example "gradient"
    method = db.StringField(required=True)  # example "uff"
    basis = db.StringField()
    molecule = db.LazyReferenceField(Molecule, required=True)

    # This is a special case where Keywords are denormalized intentionally as they are part of the
    # lookup for a single result and querying a result will not often request the keywords (LazyReference)
    keywords = db.LazyReferenceField(Keywords)

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


class Procedure(BaseResult):
    """
        A procedure is a group of related results applied to a list of molecules
    """

    procedure = db.StringField(required=True)
    program = db.StringField(required=True)  # example: 'Geometric'
    hash_index = db.StringField(required=True)

    # Unlike Results Keywords are not denormalized here as a Procedure query will always want the
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


# ================== Types of Procedures ================== #


class OptimizationProcedure(Procedure):
    """
        An Optimization  procedure
    """

    procedure = db.StringField(default='optimization', required=True)

    initial_molecule = db.LazyReferenceField(Molecule)
    final_molecule = db.LazyReferenceField(Molecule)


class TorsiondriveProcedure(Procedure):
    """
        An torsion drive  procedure
    """

    procedure = db.StringField(default='torsiondrive', required=True)

    # TODO: add more fields

    meta = {'indexes': []}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Spec(db.DynamicEmbeddedDocument):
    """ The spec of a task in the queue
        This is an embedded document, meaning that it will be embedded
        in the task_queue collection and won't be stored as a seperate
        collection/table --> for faster parsing
    """

    function = db.StringField()
    args = db.DynamicField()  # fast, can take any structure
    kwargs = db.DynamicField()


class TaskQueue(CustomDynamicDocument):
    """A queue of tasks corresponding to a procedure

       Notes: don't sort query results without having the index sorted
              will impact the performce
    """

    # spec = db.EmbeddedDocumentField(Spec, default=Spec)
    spec = db.DynamicField()

    # others
    hooks = db.ListField(db.DynamicField())  # ??
    tag = db.StringField(default=None)
    parser = db.StringField(default='')
    status = db.StringField(default='WAITING', choices=['RUNNING', 'WAITING', 'ERROR', 'COMPLETE'])
    manager = db.StringField(default=None)

    created_on = db.DateTimeField(required=True)
    modified_on = db.DateTimeField(required=True)

    # can reference Results or any Procedure
    base_result = db.GenericLazyReferenceField(dbref=True)  # use res.id and res.document_type (class)

    meta = {
        'indexes': [
            'created_on',
            'status',
            'manager',
            {
                'fields': ('base_result', ),
                'unique': True
            },  # new
        ]
    }

    def save(self, *args, **kwargs):
        """Override save to update modified_on"""
        self.modified_on = datetime.datetime.utcnow()
        if not self.created_on:
            self.created_on = datetime.datetime.utcnow()

        return super(TaskQueue, self).save(*args, **kwargs)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ServiceQueue(CustomDynamicDocument):

    status = db.StringField(default='WAITING', choices=['RUNNING', 'WAITING', 'ERROR', 'COMPLETE'])
    tag = db.StringField(default=None)
    hash_index = db.StringField(required=True)
    procedure_id = db.LazyReferenceField(Procedure)

    # created_on = db.DateTimeField(required=True)
    # modified_on = db.DateTimeField(required=True)

    meta = {
        'indexes': [
            'status',
            {
                'fields': ("status", "tag", "hash_index"),
                'unique': False
            },
            # {'fields': ('procedure',), 'unique': True}
        ]
    }

    def save(self, *args, **kwargs):
        """Override save to update modified_on"""
        # self.modified_on = datetime.datetime.utcnow()
        # if not self.created_on:
        #     self.created_on = datetime.datetime.utcnow()

        return super(ServiceQueue, self).save(*args, **kwargs)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class User(CustomDynamicDocument):

    username = db.StringField(required=True, unique=True)
    password = db.BinaryField(required=True)
    permissions = db.ListField()

    meta = {'indexes': ['username']}


class QueueManager(CustomDynamicDocument):
    """
    """

    name = db.StringField(unique=True)
    cluster = db.StringField()
    hostname = db.StringField()
    uuid = db.StringField()
    tag = db.StringField()

    # counts
    completed = db.IntField(default=0)
    submitted = db.IntField(default=0)
    failures = db.IntField(default=0)
    returned = db.IntField(default=0)

    status = db.StringField(default='INACTIVE', choices=['ACTIVE', 'INACTIVE'])

    created_on = db.DateTimeField(required=True)
    modified_on = db.DateTimeField(required=True)

    meta = {'indexes': ['status', 'name', 'modified_on']}

    def save(self, *args, **kwargs):
        """Override save to update modified_on"""
        self.modified_on = datetime.datetime.utcnow()
        if not self.created_on:
            self.created_on = datetime.datetime.utcnow()

        return super(QueueManager, self).save(*args, **kwargs)
