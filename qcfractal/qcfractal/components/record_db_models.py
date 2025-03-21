from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    ForeignKeyConstraint,
    Enum,
    TIMESTAMP,
    JSON,
    Index,
    Boolean,
    LargeBinary,
    UniqueConstraint,
    DDL,
    event,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, deferred
from sqlalchemy.orm.collections import attribute_keyed_dict

from qcfractal.components.auth.db_models import UserORM, GroupORM, UserIDMapSubquery, GroupIDMapSubquery
from qcfractal.components.managers.db_models import ComputeManagerORM
from qcfractal.db_socket import BaseORM
from qcportal.compression import CompressionEnum, decompress
from qcportal.record_models import RecordStatusEnum, OutputTypeEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from typing import Dict, Any, Optional, Iterable


class RecordCommentORM(BaseORM):
    """
    Table for storing comments on calculations
    """

    __tablename__ = "record_comment"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), default=now_at_utc, nullable=False)
    comment = Column(String, nullable=False)

    user_id = Column(Integer, ForeignKey(UserORM.id), nullable=True)
    user = relationship(
        UserIDMapSubquery,
        foreign_keys=[user_id],
        primaryjoin="RecordCommentORM.user_id == UserIDMapSubquery.id",
        lazy="selectin",
    )

    __table_args__ = (Index("ix_record_comment_record_id", "record_id"),)

    _qcportal_model_excludes = ["user_id", "user"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        d["username"] = self.user.username if self.user is not None else None
        return d


class RecordInfoBackupORM(BaseORM):
    """
    Table for storing backup info about a record

    This stores previous tag, status, priority, etc, for a record. This is used when undoing
    delete, canceling, etc.
    """

    __tablename__ = "record_info_backup"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)
    old_status = Column(Enum(RecordStatusEnum), nullable=False)
    old_compute_tag = Column(String, nullable=True)
    old_compute_priority = Column(Integer, nullable=True)
    modified_on = Column(TIMESTAMP(timezone=True), default=now_at_utc, nullable=False)

    __table_args__ = (Index("ix_record_info_backup_record_id", "record_id"),)

    _qcportal_model_excludes = ["id", "record_id"]


class OutputStoreORM(BaseORM):
    """
    Table for storing raw computation outputs (text) and errors (json)
    """

    __tablename__ = "output_store"

    id = Column(Integer, primary_key=True)
    history_id = Column(Integer, ForeignKey("record_compute_history.id", ondelete="cascade"), nullable=False)

    output_type = Column(Enum(OutputTypeEnum), nullable=False)
    compression_type = Column(Enum(CompressionEnum), nullable=False)
    compression_level = Column(Integer, nullable=False)
    data = deferred(Column(LargeBinary, nullable=False))

    __table_args__ = (UniqueConstraint("history_id", "output_type", name="ux_output_store_id_type"),)

    _qcportal_model_excludes = ["id", "history_id", "compression_level"]

    def get_output(self) -> Any:
        return decompress(self.data, self.compression_type)


# Mark the storage of the data column as external
event.listen(
    OutputStoreORM.__table__,
    "after_create",
    DDL("ALTER TABLE output_store ALTER COLUMN data SET STORAGE EXTERNAL").execute_if(dialect=("postgresql")),
)


class NativeFileORM(BaseORM):
    """
    Table for storing raw, program-dependent raw data
    """

    __tablename__ = "native_file"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)

    name = Column(String, nullable=False)
    compression_type = Column(Enum(CompressionEnum), nullable=False)
    compression_level = Column(Integer, nullable=False)
    data = deferred(Column(LargeBinary, nullable=False))

    __table_args__ = (UniqueConstraint("record_id", "name", name="ux_native_file_record_id_name"),)

    _qcportal_model_excludes = ["id", "record_id", "compression_level"]

    def get_file(self) -> Any:
        return decompress(self.data, self.compression_type)


# Mark the storage of the data column of native files as external
event.listen(
    NativeFileORM.__table__,
    "after_create",
    DDL("ALTER TABLE native_file ALTER COLUMN data SET STORAGE EXTERNAL").execute_if(dialect=("postgresql")),
)


class RecordComputeHistoryORM(BaseORM):
    """
    Table for storing the computation history of a record

    The computation history stores the result status, provenance, and manager info that
    ran a computation. This is useful for storing the history of records that have errored multiple
    times.
    """

    __tablename__ = "record_compute_history"

    id = Column(Integer, primary_key=True)
    record_id = Column(Integer, ForeignKey("base_record.id", ondelete="cascade"), nullable=False)

    status = Column(Enum(RecordStatusEnum), nullable=False)
    manager_name = Column(String, ForeignKey(ComputeManagerORM.name), nullable=True)
    modified_on = Column(TIMESTAMP(timezone=True), default=now_at_utc, nullable=False)
    provenance = Column(JSON)

    outputs = relationship(
        OutputStoreORM,
        collection_class=attribute_keyed_dict("output_type"),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("ix_record_compute_history_record_id", "record_id"),
        Index("ix_record_compute_history_manager_name", "manager_name"),
    )


class BaseRecordORM(BaseORM):
    """
    Base class for all the kinds of records
    """

    __tablename__ = "base_record"

    # for SQLAlchemy inheritence
    record_type = Column(String(100), nullable=False)

    # Some records can be either a service or a procedure
    is_service = Column(Boolean, nullable=False)

    # Base identification
    id = Column(Integer, primary_key=True)

    # Extra fields
    extras = Column(JSONB, nullable=False, default=dict)

    # Compute status
    # (Denormalized from compute history table for faster lookup during manager claiming/returning)
    status = Column(Enum(RecordStatusEnum), nullable=False)
    manager_name = Column(String, ForeignKey("compute_manager.name"), nullable=True)

    created_on = Column(TIMESTAMP(timezone=True), default=now_at_utc, nullable=False)
    modified_on = Column(TIMESTAMP(timezone=True), default=now_at_utc, nullable=False)

    # Ownership of this record
    owner_user_id = Column(Integer, ForeignKey(UserORM.id), nullable=True)
    owner_group_id = Column(Integer, ForeignKey(GroupORM.id), nullable=True)

    owner_user = relationship(
        UserIDMapSubquery,
        foreign_keys=[owner_user_id],
        primaryjoin="BaseRecordORM.owner_user_id == UserIDMapSubquery.id",
        lazy="selectin",
        viewonly=True,
    )

    owner_group = relationship(
        GroupIDMapSubquery,
        foreign_keys=[owner_group_id],
        primaryjoin="BaseRecordORM.owner_group_id == GroupIDMapSubquery.id",
        lazy="selectin",
        viewonly=True,
    )

    # Full compute history
    compute_history = relationship(
        RecordComputeHistoryORM,
        order_by=RecordComputeHistoryORM.modified_on.asc(),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    comments = relationship(
        RecordCommentORM, order_by=RecordCommentORM.timestamp.asc(), cascade="all, delete-orphan", passive_deletes=True
    )

    # Related task. The foreign key is in the task_queue table
    task = relationship(
        "TaskQueueORM", back_populates="record", uselist=False, cascade="all, delete-orphan", passive_deletes=True
    )

    # Related service. The foreign key is in the service_queue table
    service = relationship(
        "ServiceQueueORM", back_populates="record", uselist=False, cascade="all, delete-orphan", passive_deletes=True
    )

    # Backed-up info (used for undelete, etc)
    info_backup = relationship(
        RecordInfoBackupORM,
        order_by=RecordInfoBackupORM.modified_on.asc(),
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    # Various computed properties and stuff
    properties = Column(JSONB)

    # Native files returned from the computation
    native_files = relationship(
        NativeFileORM, collection_class=attribute_keyed_dict("name"), cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        Index("ix_base_record_status", "status"),
        Index("ix_base_record_record_type", "record_type"),
        Index("ix_base_record_manager_name", "manager_name"),
        Index("ix_base_record_owner_user_id", "owner_user_id"),
        Index("ix_base_record_owner_group_id", "owner_group_id"),
        Index("ix_base_record_created_on", "created_on"),
        Index("ix_base_record_modified_on", "modified_on"),
        ForeignKeyConstraint(
            ["owner_user_id", "owner_group_id"],
            ["user_groups.user_id", "user_groups.group_id"],
        ),
    )

    __mapper_args__ = {"polymorphic_on": "record_type"}

    # strip user/group ids
    # info_backup is also never part of models
    _qcportal_model_excludes = ["owner_user_id", "owner_group_id", "info_backup"]

    def model_dict(self, exclude: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        d = BaseORM.model_dict(self, exclude)

        d["owner_user"] = self.owner_user.username if self.owner_user is not None else None
        d["owner_group"] = self.owner_group.groupname if self.owner_group is not None else None

        return d

    @property
    def short_description(self) -> str:
        """
        Returns a short string describing the record
        """

        return f"short_description not implemented for {self.record_type}"


# Function for deleting large binary when derived classes are deleted
_del_baserecord_triggerfunc = DDL(
    """
    CREATE OR REPLACE FUNCTION public.qca_base_record_delete()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
        BEGIN
          DELETE FROM base_record WHERE base_record.id = OLD.id;
          RETURN OLD;
        END
        $_$
    ;
"""
)

event.listen(BaseRecordORM.__table__, "after_create", _del_baserecord_triggerfunc.execute_if(dialect=("postgresql")))
