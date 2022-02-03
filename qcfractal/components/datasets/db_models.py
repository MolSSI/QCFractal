from sqlalchemy import Column, Integer, String, JSON, Boolean, Index, ForeignKey

from qcfractal.db_socket import BaseORM, MsgpackExt


class CollectionORM(BaseORM):
    """
    A base collection class of precomuted workflows such as datasets, ..

    This is a dynamic document, so it will accept any number of
    extra fields (expandable and uncontrolled schema)
    """

    __tablename__ = "collection"

    id = Column(Integer, primary_key=True)
    collection_type = Column(String, nullable=False)  # for inheritance

    collection = Column(String(100), nullable=False)
    lname = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)

    tags = Column(JSON)
    tagline = Column(String)
    description = Column(String)

    group = Column(String(100), nullable=False)
    visibility = Column(Boolean, nullable=False)

    default_tag = Column(String, nullable=False)
    default_priority = Column(Integer, nullable=False)

    provenance = Column(JSON)

    extra = Column(JSON)  # extra data related to specific collection type

    def update_relations(self, **kwarg):
        pass

    __table_args__ = (
        Index("ix_collection_lname", "collection", "lname", unique=True),
        Index("ix_collection_type", "collection_type"),
    )

    __mapper_args__ = {"polymorphic_on": "collection_type", "polymorphic_identity": "collection"}


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


class ContributedValuesORM(BaseORM):
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
