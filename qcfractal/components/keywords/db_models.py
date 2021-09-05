from sqlalchemy import Column, Integer, String, JSON, Boolean, Index

from qcfractal.interface.models import ObjectId
from qcfractal.db_socket import Base

from typing import Dict, Any, Optional, Iterable


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

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:

        d = Base.dict(self, exclude)

        # TODO - INT ID should not be done
        if "id" in d:
            d["id"] = ObjectId(d["id"])

        return d
