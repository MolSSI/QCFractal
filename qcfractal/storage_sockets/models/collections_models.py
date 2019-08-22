from sqlalchemy import (Column, Integer, String, ForeignKey, JSON, Index)
from sqlalchemy.orm import relationship, column_property
# from sqlalchemy import select, func, tuple_, text, cast
from qcfractal.storage_sockets.models import Base
# from sqlalchemy.dialects.postgresql import array_agg
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.ext.hybrid import hybrid_property


# class json_agg(GenericFunction):
#     type = postgresql.JSON

class json_build_object(GenericFunction):
    type = postgresql.JSON


class CollectionORM(Base):
    """
        A base collection class of precomuted workflows such as datasets, ..

        This is a dynamic document, so it will accept any number of
        extra fields (expandable and uncontrolled schema)
    """

    __tablename__ = "collection"

    id = Column(Integer, primary_key=True)
    collection_type = Column(String)  # for inheritance

    collection = Column(String(100), nullable=False)
    lname = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)

    tags = Column(JSON)
    tagline = Column(String)

    provenance = Column(JSON)

    extra = Column(JSON)  # extra data related to specific collection type

    def update_relations(self, **kwarg):
        pass


    __table_args__ = (
        Index('ix_collection_lname', "collection", "lname", unique=True),
        Index('ix_collection_type', 'collection_type'),
    )

    __mapper_args__ = {'polymorphic_on': 'collection_type'}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class DatasetMixin:
    """
        Mixin class for common Dataset attributes.
    """

    default_benchmark = Column(String, nullable=True)
    default_keywords  = Column(JSON, nullable=True)

    default_driver = Column(String, nullable=True)
    default_units = Column(String, nullable=True)
    alias_keywords = Column(JSON, nullable=True)
    default_program  = Column(String, nullable=True)

    contributed_values = Column(JSON)

    history_keys = Column(JSON)
    history = Column(JSON)


class DatasetEntryORM(Base):
    """Association table for many to many"""

    __tablename__ = 'dataset_entry'

    dataset_id = Column(Integer, ForeignKey('dataset.id', ondelete='cascade'), primary_key=True)
    #TODO: check the cascase_delete with molecule
    molecule_id = Column(Integer, ForeignKey('molecule.id'), primary_key=True)

    name = Column(String, nullable=False)
    comment = Column(String)
    local_results = Column(JSON)


class DatasetORM(CollectionORM, DatasetMixin):
    """
        The Dataset class for homogeneous computations on many molecules.
    """

    __tablename__ = "dataset"

    id = Column(Integer, ForeignKey('collection.id', ondelete="CASCADE"), primary_key=True)

    # records: [{"name": "He1", "molecule_id": "1", "comment": null, "local_results": {}},
    #             {"name": "He2", "molecule_id": "2", "comment": null, "local_results": {}}],
    # records = column_property(
    #     select([func.json_agg(tuple_(
    #           DatasetRecordsAssociation.molecule_id, DatasetRecordsAssociation.name
    #
    #     ))])
    #         .where(DatasetRecordsAssociation.dataset_id == id))

    ## returns json strings
    # records = column_property(
    #     select([array_agg(cast(json_build_object(
    #         "molecule_id", DatasetRecordsORM.molecule_id,
    #         "name", DatasetRecordsORM.name,
    #         "comment", DatasetRecordsORM.comment,
    #         "local_results", DatasetRecordsORM.local_results
    #     ), type_=JSON))])
    #         # .select_from(DatasetRecordsAssociation.__tablename__) # doesn't work
    #         .where(DatasetRecordsORM.dataset_id == id))  #, deferred=True)


    records_obj = relationship(DatasetEntryORM,
                               lazy='selectin',   #lazy='noload', # when using column_property
                               cascade="all, delete-orphan",
                               backref="dataset")

    @hybrid_property
    def records(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation"""

        ret = []
        try:
            for rec in self.records_obj:
                ret.append(rec.to_dict(exclude=['dataset_id']))
        except Exception as err:
            # raises exception of first access!!
            pass

        return ret

    @records.setter
    def records(self, dict_values):
        return dict_values

    def update_relations(self, records=None, **kwarg):

        self.records_obj = []
        records = [] if not records else records
        for rec_dict in records:
            rec = DatasetEntryORM(dataset_id=int(self.id),**rec_dict)
            self.records_obj.append(rec)


    __table_args__ = (
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        # UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name='uix_results_keys'),
    )


    __mapper_args__ = {
        'polymorphic_identity': 'dataset',
        # to have separate select when querying CollectionORM
        'polymorphic_load': 'selectin',
    }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ReactionDatasetEntryORM(Base):
    """Association table for many to many"""

    __tablename__ = 'reaction_dataset_entry'

    reaction_dataset_id = Column(Integer, ForeignKey('reaction_dataset.id', ondelete='cascade'), primary_key=True)

    attributes = Column(JSON)
    name = Column(String, nullable=False, primary_key=True)
    reaction_results = Column(JSON)
    stoichiometry = Column(JSON)
    extras = Column(JSON)


class ReactionDatasetORM(CollectionORM, DatasetMixin):
    """
        Reaction Dataset
    """

    __tablename__ = "reaction_dataset"

    id = Column(Integer, ForeignKey('collection.id', ondelete="CASCADE"), primary_key=True)

    ds_type  = Column(String, nullable=True)

    records_obj = relationship(ReactionDatasetEntryORM,
                               lazy='selectin',
                               cascade="all, delete-orphan",
                               backref="reaction_dataset")

    def update_relations(self, records=None, **kwarg):

        self.records_obj = []
        records = records or []
        for rec_dict in records:
            rec = ReactionDatasetEntryORM(reaction_dataset_id=int(self.id),**rec_dict)
            self.records_obj.append(rec)

    @hybrid_property
    def records(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation"""

        ret = []
        try:
            for rec in self.records_obj:
                ret.append(rec.to_dict(exclude=['reaction_dataset_id']))
        except Exception as err:
            # raises exception of first access!!
            pass
        return ret

    @records.setter
    def records(self, dict_values):
        return dict_values


    __table_args__ = (
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        # UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name='uix_results_keys'),
    )


    __mapper_args__ = {
        'polymorphic_identity': 'reactiondataset',
        # to have separate select when querying CollectionORM
        'polymorphic_load': 'selectin',
    }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
