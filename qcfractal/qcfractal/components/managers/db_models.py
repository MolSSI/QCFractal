from sqlalchemy import (
    Column,
    Integer,
    ForeignKey,
    TIMESTAMP,
    Float,
    Index,
    String,
    JSON,
    Enum,
    UniqueConstraint,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship

from qcfractal.db_socket import BaseORM
from qcportal.managers import ManagerStatusEnum
from qcportal.utils import now_at_utc


class ComputeManagerORM(BaseORM):
    """
    Table for storing information about active and inactive compute managers
    """

    __tablename__ = "compute_manager"

    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    cluster = Column(String, nullable=False)
    hostname = Column(String, nullable=False)
    username = Column(String)  # Can be null (ie, security not enabled, snowflake)
    tags = Column(ARRAY(String), nullable=False)

    # Latest count
    claimed = Column(Integer, nullable=False, default=0)
    successes = Column(Integer, nullable=False, default=0)
    failures = Column(Integer, nullable=False, default=0)
    rejected = Column(Integer, nullable=False, default=0)

    active_tasks = Column(Integer, nullable=False, default=0)
    active_cores = Column(Integer, nullable=False, default=0)
    active_memory = Column(Float, nullable=False, default=0.0)
    total_cpu_hours = Column(Float, nullable=False, default=0.0)

    status = Column(Enum(ManagerStatusEnum), nullable=False)

    created_on = Column(TIMESTAMP(timezone=True), nullable=False, default=now_at_utc)
    modified_on = Column(TIMESTAMP(timezone=True), nullable=False, default=now_at_utc)

    manager_version = Column(String, nullable=False)
    programs = Column(JSON, nullable=False)

    __table_args__ = (
        Index("ix_compute_manager_status", "status"),
        Index("ix_compute_manager_modified_on", "modified_on", postgresql_using="brin"),
        UniqueConstraint("name", name="ux_compute_manager_name"),
        CheckConstraint("programs::text = LOWER(programs::text)", name="ck_compute_manager_programs_lower"),
        CheckConstraint("tags::text = LOWER(tags::text)", name="ck_compute_manager_tags_lower"),
    )
