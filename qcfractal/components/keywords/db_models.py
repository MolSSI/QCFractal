from sqlalchemy import Column, Integer, String, JSON, Boolean, Index

from qcfractal.interface.models import ObjectId
from qcfractal.storage_sockets.models import Base


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

    __table_args__ = (Index("ix_keywords_hash_index", "hash_index", unique=True),)

    def dict(self):

        d = Base.dict(self)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d
