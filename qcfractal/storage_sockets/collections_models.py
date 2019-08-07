import datetime
from sqlalchemy import (Column, Integer, String, DateTime, Boolean, ForeignKey, JSON, Enum, Float, Binary, Table,
                        inspect, Index, UniqueConstraint)
from sqlalchemy.orm import relationship, column_property

from qcfractal.storage_sockets.sql_base import Base
from qcfractal.storage_sockets.sql_models import MoleculeORM


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
    extra = Column(JSON)  # extra data related to specific collection type

    __table_args__ = (
        Index('ix_collection_lname', "collection", "lname", unique=True),
        Index('ix_collection_type', 'collection_type'),
    )

    __mapper_args__ = {'polymorphic_on': 'collection_type'}

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class DatasetRecordsAssociation(Base):
    """Association table for many to many"""

    __tablename__ = 'dataset_records_association'

    dataset_id = Column(Integer, ForeignKey('dataset.id', ondelete='cascade'), primary_key=True)
    #TODO: check the cascase_delete with molecule
    molecule_id = Column(Integer, ForeignKey('molecule.id', ondelete='cascade'), primary_key=True)

    name = Column(String, nullable=False)
    comment = Column(String)
    local_results = Column(String)


class DatasetORM(CollectionORM):
    """
        The Dataset class for homogeneous computations on many molecules.
    """

    __tablename__ = "dataset"

    id = Column(Integer, ForeignKey('collection.id', ondelete="CASCADE"), primary_key=True)


    default_benchmark = Column(String, nullable=True)
    default_keywords  = Column(String, nullable=True)

    default_driver = Column(String, nullable=True)
    default_units = Column(String, nullable=True)
    alias_keywords = Column(String, nullable=True)
    default_program  = Column(String, nullable=True)

    # contributed_values: {"gradient": {"name": "Gradient", "theory_level": "pseudo-random values", "values": {"He1": [0.03, 0, 0.02, -0.02, 0, -0.03], "He2": [0.03, 0, 0.02, -0.02, 0, -0.03]}, "units": "hartree", "doi": null, "theory_level_details": null, "comments": null}},
    contributed_values = Column(JSON)

    provenance = Column(JSON)  # TODO: in extra?

    # history_keys: ["driver", "program", "method", "basis", "keywords"],
    # history: [["gradient", "psi4", "hf", "sto-3g", null]],
    history_keys = Column(JSON)
    history = Column(JSON)

    # records: [{"name": "He1", "molecule_id": "1", "comment": null, "local_results": {}},
    #             {"name": "He2", "molecule_id": "2", "comment": null, "local_results": {}}],
    records_obj = relationship(DatasetRecordsAssociation,
                               lazy='selectin',
                               cascade="all, delete-orphan",
                               backref="dataset")

    extra = Column(JSON)  # extra data related to specific collection type

    __table_args__ = (
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        # UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name='uix_results_keys'),
    )


    __mapper_args__ = {
        'polymorphic_identity': 'dataset',
        # to have separate select when querying CollectionORM
        'polymorphic_load': 'selectin',
    }
