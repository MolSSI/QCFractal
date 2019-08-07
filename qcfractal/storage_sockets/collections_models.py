import datetime
from sqlalchemy import (Column, Integer, String, DateTime, Boolean, ForeignKey, JSON, Enum, Float, Binary, Table,
                        inspect, Index)

from qcfractal.storage_sockets.sql_base import Base
from qcfractal.storage_sockets.sql_models import MoleculeORM


class CollectionORM(Base):
    """
        A collection of precomuted workflows such as datasets, ..

        This is a dynamic document, so it will accept any number of
        extra fields (expandable and uncontrolled schema)
    """

    __tablename__ = "collection"

    id = Column(Integer, primary_key=True)

    collection = Column(String(100), nullable=False)
    lname = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)

    tags = Column(JSON)
    tagline = Column(String)
    extra = Column(JSON)  # extra data related to specific collection type

    __table_args__ = (Index('ix_collection_lname', "collection", "lname", unique=True), )

    # meta = {
    #     'indexes': [{
    #         'fields': ('collection', 'name'),
    #         'unique': True
    #     }]
    # }


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~