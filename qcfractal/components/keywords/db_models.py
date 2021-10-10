from sqlalchemy import Column, Integer, String, JSON, Boolean, UniqueConstraint

from qcfractal.db_socket import BaseORM


class KeywordsORM(BaseORM):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    hash_index = Column(String, nullable=False)
    values = Column(JSON, nullable=False)

    lowercase = Column(Boolean, nullable=False, default=True)
    exact_floats = Column(Boolean, nullable=False, default=False)
    comments = Column(String)

    __table_args__ = (UniqueConstraint("hash_index", name="ux_keywords_hash_index"),)
