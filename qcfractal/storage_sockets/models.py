import mongoengine as db
import datetime
import bson


class Collection(db.DynamicDocument):
    """
        A collection of precomuted workflows such as datasets, ..

        This is a dynamic document, so it will accept any number of
        extra fields (expandable and uncontrolled schema)
    """

    collection = db.StringField(required=True)  # , choices=['dataset', '?'])
    name = db.StringField(required=True)  # Example 'water'

    meta = {
        'collection': 'collections',  # DB collection/table name
        'indexes': [
            {'fields': ('collection', 'name'), 'unique': True}
        ]
    }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Molecule(db.DynamicDocument):
    """
        The molecule DB collection is managed by pymongo, so far
    """

    name = db.StringField()
    symbols = db.ListField()
    molecular_formula = db.StringField()
    molecule_hash = db.StringField()
    geometry = db.ListField()
    real = db.ListField()
    fragments = db.DynamicField()

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
        'collection': 'molecules',
        'indexes': [
            {'fields': ('molecule_hash', 'molecular_formula'),
             'unique': False
            }  # TODO: what is unique?
        ]
    }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Options(db.DynamicDocument):
    """
        Options are unique for a specific program and name
    """

    # TODO: pull choices from const config
    program = db.StringField(required=True) #, choices=['rdkit', 'psi4', 'geometric', 'torsiondrive'])
    # "default is reserved, insert on start
    # option_name = db.StringField(required=True)
    name = db.StringField(required=True)

    meta = {
        'indexes': [
            {'fields': ('program', 'name'), 'unique': True}
        ]
    }

    def __str__(self):
        return str(self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class BaseResult(db.DynamicDocument):
    """
        Abstract Base class for Results and Procedures
    """

    # queue related
    task_queue_id = db.StringField()  # ObjectId, reference task_queue but without validation
    status = db.StringField(required=True, choices=['COMPLETE', 'INCOMPLETE', 'ERROR'])

    meta = {
        'abstract': True,
        # 'allow_inheritance': True,
        'indexes': [
            'status'
        ]
    }

    def save(self, *args, **kwargs):
        """Override save to set defaults"""

        if not self.status:
            self.status = 'INCOMPLETE'

        return super(BaseResult, self).save(*args, **kwargs)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Result(BaseResult):
    """
        Hold the result of an atomic single calculation
    """

    # uniquely identifying a result
    program = db.StringField(required=True)  # example "rdkit", is it the same as program in options?
    driver = db.StringField(required=True)  # example "gradient"
    method = db.StringField(required=True)  # example "uff"
    basis = db.StringField()
    molecule = db.ReferenceField(Molecule, required=True)   # or LazyReferenceField if only ID is needed?
    # options = db.ReferenceField(Options)  # ** has to be a FK or empty, can't be a string
    options = db.StringField()

    # output related
    properties = db.DynamicField()  # accept any, no validation
    return_result = db.DynamicField()  # better performance than db.ListField(db.FloatField())
    provenance = db.DynamicField()  # or an Embedded Documents with a structure?
        #  {"creator": "rdkit", "version": "2018.03.4",
        # "routine": "rdkit.Chem.AllChem.UFFGetMoleculeForceField",
        # "cpu": "Intel(R) Core(TM) i7-8650U CPU @ 1.90GHz", "hostname": "x1-carbon6", "username": "doaa",
        # "wall_time": 0.14191770553588867},

    schema_name = db.StringField() #default="qc_ret_data_output")
    schema_version = db.IntField()  # or String?

    meta = {
        'collection': 'results',
        'indexes': [
           {'fields': ('program', 'driver', 'method', 'basis',
                       'molecule', 'options'), 'unique': True},
        ]
    }

    # not used yet
    # or  use pre_save
    def _save(self, *args, **kwargs):
        """Override save to handle options"""

        if not isinstance(self.options, Options):
            # self.options = Options.objects(program=self.program, option_name='default')\
            #     .modify(upsert=True, new=True, option_name='default')
            self.options = Options.objects(program=self.program, option_name='default').first()
            if not self.options:
                self.options = Options(program=self.program, option_name='default').save()
                # self.options.save()

        return super(Result, self).save(*args, **kwargs)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Procedure(BaseResult):
    """
        A procedure is a group of related results applied to a list of molecules

        TODO: this looks exactly like results except those attributes listed here
    """

    procedure = db.StringField(required=True)
                                    # choices=['undefined', 'optimization', 'torsiondrive'])
    # Todo: change name to be different from results program
    program = db.StringField(required=True)  # example: 'Geometric'
    options = db.ReferenceField(Options)  # options of the procedure

    qc_meta = db.DynamicField()  # --> all inside results

    meta = {
        'collection': 'procedure',
        'allow_inheritance': True,
        'indexes': [
            # TODO: needs a unique index, + molecule?
            {'fields': ('procedure', 'program'), 'unique': False}  # TODO: check
        ]
    }

# ================== Types of Procedures ================== #


class OptimizationProcedure(Procedure):
    """
        An Optimization  procedure
    """

    procedure = db.StringField(default='optimization', required=True)

    # initial_molecule = db.ReferenceField(Molecule)  # always load with select_related
    # final_molecule = db.ReferenceField(Molecule)

    # output
    # trajectory = db.ListField(Result)

    meta = {
        'indexes': [
            # {'fields': ('initial_molecule', 'procedure_type', 'procedure_program'), 'unique': False}  # TODO: check
        ]
    }


class TorsiondriveProcedure(Procedure):
    """
        An torsion drive  procedure
    """

    procedure = db.StringField(default='torsiondrive', required=True)

    # TODO: add more fields

    meta = {
        'indexes': [
        ]
    }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class Spec(db.DynamicEmbeddedDocument):
    """ The spec of a task in the queue
        This is an embedded document, meaning that it will be embedded
        in the task_queue collection and won't be stored as a seperate
        collection/table --> for faster parsing
    """

    function = db.StringField()
    args = db.DynamicField()    # fast, can take any structure
    kwargs = db.DynamicField()


class TaskQueue(db.DynamicDocument):
    """A queue of tasks corresponding to a procedure"""

    # spec = db.EmbeddedDocumentField(Spec, default=Spec)
    spec = db.DynamicField()

    # others
    hooks = db.ListField(db.DynamicField())  # ??
    tag = db.StringField(default=None)
    parser = db.StringField(default='')
    status = db.StringField(default='WAITING')
                            # choices=['RUNNING', 'WAITING', 'ERROR', 'COMPLETE'])

    created_on = db.DateTimeField(required=True, default=datetime.datetime.now)
    modified_on = db.DateTimeField(required=True, default=datetime.datetime.now)

    base_result = db.GenericLazyReferenceField(dbref=True)  # GenericLazyReferenceField()  # can reference Results or any Procedure

    meta = {
        'indexes': [
            '-created_on',
            'status',
            # {'fields': ("status", "tag", "hash_index"), 'unique': False}
            {'fields': ("base_result",), 'unique': True}  # new

        ]
        # 'indexes': [
        #         '$function', # text index, not needed
        #         '#function', # hash index
        #         ('title', '-rating'),  # rating is descending, direction only for multi-indices
        #     {
        #       'fields': ('spec.function', 'tag'),
        #       'unique': True
        #     }
        # ]
    }

    # override to simplify the generic reference field
    def to_json(self):
        data = self.to_mongo()
        data['base_result'] = data['base_result']['_ref']
        return bson.json_util.dumps(data)

    def save(self, *args, **kwargs):
        """Override save to update modified_on"""
        self.modified_on = datetime.datetime.now()

        return super(TaskQueue, self).save(*args, **kwargs)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ServiceQueue(db.DynamicDocument):

    meta = {
        'indexes': [
            'status',
            {'fields': ("status", "tag", "hash_index"), 'unique': False}

        ]
    }
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
