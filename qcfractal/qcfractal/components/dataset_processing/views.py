from __future__ import annotations

import os
from collections import defaultdict
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from qcfractal.components.internal_jobs.status import JobProgress
from qcfractal.components.record_db_models import BaseRecordORM
from qcportal.cache import DatasetCache
from qcportal.dataset_models import BaseDataset
from qcportal.record_models import RecordStatusEnum, BaseRecord
from qcportal.utils import chunk_iterable

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from qcfractal.db_socket.socket import SQLAlchemySocket
    from typing import Optional, Iterable
    from typing import Iterable
    from sqlalchemy.orm.session import Session


def create_view_file(
    session: Session,
    socket: SQLAlchemySocket,
    dataset_id: int,
    dataset_type: str,
    output_path: str,
    status: Optional[Iterable[RecordStatusEnum]] = None,
    include: Optional[Iterable[str]] = None,
    exclude: Optional[Iterable[str]] = None,
    *,
    include_children: bool = True,
    job_progress: Optional[JobProgress] = None,
):
    """
    Creates a view file for a dataset

    Note: the job progress object will be filled to 90% to leave room for uploading

    Parameters
    ----------
    session
        An existing SQLAlchemy session to use.
    socket
        Full SQLAlchemy socket to use for getting records
    dataset_type
        Type of the underlying dataset to create a view of (as a string)
    dataset_id
        ID of the dataset to create the view for
    output_path
        Full path (including filename) to output the view data to. Must not already exist
    status
        List of statuses to include. Default is to include records with any status
    include
        List of specific record fields to include in the export. Default is to include most fields
    exclude
        List of specific record fields to exclude from the export. Defaults to excluding none.
    include_children
        Specifies whether child records associated with the main records should also be included (recursively)
        in the view file.
    job_progress
        Object used to track the progress of the job
    """

    if os.path.exists(output_path):
        raise RuntimeError(f"File {output_path} exists - will not overwrite")

    if os.path.isdir(output_path):
        raise RuntimeError(f"{output_path} is a directory")

    ds_socket = socket.datasets.get_socket(dataset_type)
    ptl_dataset_type = BaseDataset.get_subclass(dataset_type)

    ptl_entry_type = ptl_dataset_type._entry_type
    ptl_specification_type = ptl_dataset_type._specification_type

    view_db = DatasetCache(output_path, read_only=False, dataset_type=ptl_dataset_type)

    stmt = select(ds_socket.dataset_orm).where(ds_socket.dataset_orm.id == dataset_id)
    stmt = stmt.options(selectinload("*"))
    ds_orm = session.execute(stmt).scalar_one()

    # Metadata
    view_db.update_metadata("dataset_metadata", ds_orm.model_dict())

    # Entries
    if job_progress is not None:
        job_progress.raise_if_cancelled()
        job_progress.update_progress(0, "Processing dataset entries")

    stmt = select(ds_socket.entry_orm)
    stmt = stmt.options(selectinload("*"))
    stmt = stmt.where(ds_socket.entry_orm.dataset_id == dataset_id)

    entries = session.execute(stmt).scalars().all()
    entries = [e.to_model(ptl_entry_type) for e in entries]
    view_db.update_entries(entries)

    if job_progress is not None:
        job_progress.raise_if_cancelled()
        job_progress.update_progress(5, "Processing dataset specifications")

    # Specifications
    stmt = select(ds_socket.specification_orm)
    stmt = stmt.options(selectinload("*"))
    stmt = stmt.where(ds_socket.specification_orm.dataset_id == dataset_id)

    specs = session.execute(stmt).scalars().all()
    specs = [s.to_model(ptl_specification_type) for s in specs]
    view_db.update_specifications(specs)

    if job_progress is not None:
        job_progress.raise_if_cancelled()
        job_progress.update_progress(10, "Loading record information")

    # Now all the records
    stmt = select(ds_socket.record_item_orm).where(ds_socket.record_item_orm.dataset_id == dataset_id)
    stmt = stmt.order_by(ds_socket.record_item_orm.record_id.asc())
    record_items = session.execute(stmt).scalars().all()

    record_ids = set(ri.record_id for ri in record_items)
    all_ids = set(record_ids)

    if include_children:
        # Get all the children ids
        children_ids = socket.records.get_children_ids(session, record_ids)
        all_ids |= set(children_ids)

    ############################################################################
    # Determine the record types of all the ids (top-level and children if desired)
    ############################################################################
    stmt = select(BaseRecordORM.id, BaseRecordORM.record_type)

    if status is not None:
        stmt = stmt.where(BaseRecordORM.status.in_(status))

    # Sort into a dictionary with keys being the record type
    record_type_map = defaultdict(list)

    for id_chunk in chunk_iterable(all_ids, 500):
        stmt2 = stmt.where(BaseRecordORM.id.in_(id_chunk))
        for record_id, record_type in session.execute(stmt2).yield_per(100):
            record_type_map[record_type].append(record_id)

    if job_progress is not None:
        job_progress.raise_if_cancelled()
        job_progress.update_progress(15, "Processing individual records")

    ############################################################################
    # Actually fetch the record data now
    # We go one over the different types of records, then load them in batches
    ############################################################################
    record_count = len(all_ids)
    finished_count = 0

    for record_type_str, record_ids in record_type_map.items():
        record_socket = socket.records.get_socket(record_type_str)
        record_type = BaseRecord.get_subclass(record_type_str)

        for id_chunk in chunk_iterable(record_ids, 200):
            record_dicts = record_socket.get(id_chunk, include=include, exclude=exclude, session=session)
            record_data = [record_type(**r) for r in record_dicts]
            view_db.update_records(record_data)

            finished_count += len(id_chunk)
            if job_progress is not None:
                job_progress.raise_if_cancelled()

                # Fraction of the 75% left over (15 to start, 10 left over for uploading)
                job_progress.update_progress(
                    15 + int(75 * finished_count / record_count), "Processing individual records"
                )

    # Update the dataset <-> record association
    record_info = [(ri.entry_name, ri.specification_name, ri.record_id) for ri in record_items]
    view_db.update_dataset_records(record_info)
