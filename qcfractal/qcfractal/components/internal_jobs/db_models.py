from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, DateTime, String, JSON, Index, Enum, UniqueConstraint

from qcfractal.db_socket import BaseORM
from qcportal.internal_jobs.models import InternalJobStatusEnum

if TYPE_CHECKING:
    from typing import Optional, Iterable, Dict, Any


class InternalJobORM(BaseORM):
    __tablename__ = "internal_jobs"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    status = Column(Enum(InternalJobStatusEnum), nullable=False)
    added_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    scheduled_date = Column(DateTime, nullable=False)
    started_date = Column(DateTime)
    last_updated = Column(DateTime)
    ended_date = Column(DateTime)
    runner_hostname = Column(String)
    runner_uuid = Column(String)

    progress = Column(Integer, nullable=False, default=0)

    function = Column(String, nullable=False)
    kwargs = Column(JSON, nullable=False)

    after_function = Column(String, nullable=True)
    after_function_kwargs = Column(JSON, nullable=True)

    result = Column(JSON)
    user = Column(String)

    # Nullable column with unique constraint. If a unique_name is specified,
    # it must be unique. null != null always
    unique_name = Column(String, nullable=True)

    __table_args__ = (
        Index("ix_internal_jobs_added_date", "added_date", postgresql_using="brin"),
        Index("ix_internal_jobs_scheduled_date", "scheduled_date", postgresql_using="brin"),
        Index("ix_internal_jobs_last_updated", "last_updated", postgresql_using="brin"),
        Index("ix_internal_jobs_status", "status"),
        Index("ix_internal_jobs_name", "name"),
        UniqueConstraint("unique_name", name="ux_internal_jobs_unique_name"),
    )

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = self.append_exclude(exclude, "unique_name")
        return BaseORM.model_dict(self, exclude)
