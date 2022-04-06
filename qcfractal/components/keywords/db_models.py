from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, String, JSON, Boolean, UniqueConstraint

from qcfractal.db_socket import BaseORM

if TYPE_CHECKING:
    from typing import Optional, Iterable, Dict, Any


class KeywordsORM(BaseORM):
    __tablename__ = "keywords"

    id = Column(Integer, primary_key=True)
    hash_index = Column(String, nullable=False)
    values = Column(JSON, nullable=False)

    lowercase = Column(Boolean, nullable=False, default=True)
    exact_floats = Column(Boolean, nullable=False, default=False)
    comments = Column(String)

    __table_args__ = (UniqueConstraint("hash_index", name="ux_keywords_hash_index"),)

    def dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        return self.values
