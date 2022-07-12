from sqlalchemy import JSON, Boolean, Column, ForeignKey, Index, Integer, String
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.sql.functions import GenericFunction

from qcfractal.storage_sockets.models.sql_base import Base, MsgpackExt

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
    description = Column(String)

    group = Column(String(100), nullable=False)
    visibility = Column(Boolean, nullable=False)

    view_url_hdf5 = Column(String)
    view_url_plaintext = Column(String)
    view_metadata = Column(JSON)
    view_available = Column(Boolean, nullable=False)

    provenance = Column(JSON)

    extra = Column(JSON)  # extra data related to specific collection type

    def update_relations(self, **kwarg):
        pass

    __table_args__ = (
        Index("ix_collection_lname", "collection", "lname", unique=True),
        Index("ix_collection_type", "collection_type"),
    )

    __mapper_args__ = {"polymorphic_on": "collection_type"}


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class DatasetMixin:
    """
    Mixin class for common Dataset attributes.
    """

    default_benchmark = Column(String)
    default_keywords = Column(JSON)

    default_driver = Column(String)
    default_units = Column(String)
    alias_keywords = Column(JSON)
    default_program = Column(String)

    history_keys = Column(JSON)
    history = Column(JSON)


class ContributedValuesORM(Base):
    """One group of a contibuted values per dataset
    Each dataset can have multiple rows in this table"""

    __tablename__ = "contributed_values"

    collection_id = Column(Integer, ForeignKey("collection.id", ondelete="cascade"), primary_key=True)

    name = Column(String, nullable=False, primary_key=True)
    values = Column(MsgpackExt, nullable=False)
    index = Column(MsgpackExt, nullable=False)
    values_structure = Column(JSON, nullable=False)

    theory_level = Column(JSON, nullable=False)
    units = Column(String, nullable=False)
    theory_level_details = Column(JSON)

    citations = Column(JSON)
    external_url = Column(String)
    doi = Column(String)

    comments = Column(String)


class DatasetEntryORM(Base):
    """Association table for many to many"""

    __tablename__ = "dataset_entry"

    dataset_id = Column(Integer, ForeignKey("dataset.id", ondelete="cascade"), primary_key=True)
    # TODO: check the cascase_delete with molecule
    molecule_id = Column(Integer, ForeignKey("molecule.id"), nullable=False)

    name = Column(String, nullable=False, primary_key=True)
    comment = Column(String)
    local_results = Column(JSON)


class DatasetORM(CollectionORM, DatasetMixin):
    """
    The Dataset class for homogeneous computations on many molecules.
    """

    __tablename__ = "dataset"

    id = Column(Integer, ForeignKey("collection.id", ondelete="CASCADE"), primary_key=True)

    contributed_values_obj = relationship(ContributedValuesORM, lazy="selectin", cascade="all, delete-orphan")

    records_obj = relationship(
        DatasetEntryORM, lazy="selectin", cascade="all, delete-orphan", backref="dataset"  # lazy='noload',
    )

    @hybrid_property
    def contributed_values(self):
        return self._contributed_values(self.contributed_values_obj)

    @staticmethod
    def _contributed_values(contributed_values_obj):
        if not contributed_values_obj:
            return {}

        if not isinstance(contributed_values_obj, list):
            contributed_values_obj = [contributed_values_obj]
        ret = {}
        try:
            for obj in contributed_values_obj:
                ret[obj.name.lower()] = obj.to_dict(exclude=["collection_id"])
        except Exception as err:
            pass

        return ret

    @contributed_values.setter
    def contributed_values(self, dict_values):
        return dict_values

    @hybrid_property
    def records(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation"""

        return self._records(self.records_obj)

    @staticmethod
    def _records(records_obj):

        if not records_obj:
            return []

        if not isinstance(records_obj, list):
            records_obj = [records_obj]

        ret = []
        try:
            for rec in records_obj:
                ret.append(rec.to_dict(exclude=["dataset_id"]))
        except Exception as err:
            # raises exception of first access!!
            pass

        return ret

    @records.setter
    def records(self, dict_values):
        return dict_values

    def update_relations(self, records=None, contributed_values=None, **kwarg):

        self.records_obj = []
        records = [] if not records else records
        for rec_dict in records:
            rec = DatasetEntryORM(dataset_id=int(self.id), **rec_dict)
            self.records_obj.append(rec)

        self.contributed_values_obj = []
        contributed_values = {} if not contributed_values else contributed_values
        for key, rec_dict in contributed_values.items():
            rec = ContributedValuesORM(collection_id=int(self.id), **rec_dict)
            self.contributed_values_obj.append(rec)

    __table_args__ = (
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        # UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name='uix_results_keys'),
    )

    __mapper_args__ = {
        "polymorphic_identity": "dataset",
        # to have separate select when querying CollectionORM
        "polymorphic_load": "selectin",
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class ReactionDatasetEntryORM(Base):
    """Association table for many to many"""

    __tablename__ = "reaction_dataset_entry"

    reaction_dataset_id = Column(Integer, ForeignKey("reaction_dataset.id", ondelete="cascade"), primary_key=True)

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

    id = Column(Integer, ForeignKey("collection.id", ondelete="CASCADE"), primary_key=True)

    ds_type = Column(String)

    records_obj = relationship(
        ReactionDatasetEntryORM, lazy="selectin", cascade="all, delete-orphan", backref="reaction_dataset"
    )

    contributed_values_obj = relationship(ContributedValuesORM, lazy="selectin", cascade="all, delete-orphan")

    @hybrid_property
    def contributed_values(self):
        return self._contributed_values(self.contributed_values_obj)

    @staticmethod
    def _contributed_values(contributed_values_obj):
        return DatasetORM._contributed_values(contributed_values_obj)

    @contributed_values.setter
    def contributed_values(self, dict_values):
        return dict_values

    def update_relations(self, records=None, contributed_values=None, **kwarg):

        self.records_obj = []
        records = records or []
        for rec_dict in records:
            rec = ReactionDatasetEntryORM(reaction_dataset_id=int(self.id), **rec_dict)
            self.records_obj.append(rec)

        self.contributed_values_obj = []
        contributed_values = {} if not contributed_values else contributed_values
        for key, rec_dict in contributed_values.items():
            rec = ContributedValuesORM(collection_id=int(self.id), **rec_dict)
            self.contributed_values_obj.append(rec)

    @hybrid_property
    def records(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation"""

        return self._records(self.records_obj)

    @staticmethod
    def _records(records_obj):

        if not records_obj:
            return []

        if not isinstance(records_obj, list):
            records_obj = [records_obj]

        ret = []
        try:
            for rec in records_obj:
                ret.append(rec.to_dict(exclude=["reaction_dataset_id"]))
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
        "polymorphic_identity": "reactiondataset",
        # to have separate select when querying CollectionORM
        "polymorphic_load": "selectin",
    }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
