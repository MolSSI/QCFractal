"""
Backfills stdout/stderr for errored records whose output was nested inside
error.extras["failed_result"] rather than surfaced as a top-level stdout/stderr output.

This affects records that errored via a procedure (eg geomeTRIC, optking) wrapping a
failing sub-computation, before the fix to RecordSocket.update_failed_task. Their
compute-history error output already contains the useful stdout/stderr - it's just
nested rather than promoted to its own output row - so this script is a pure data
backfill. It does not require an alembic migration and is safe to run multiple times
(only history rows still missing a stdout output are candidates; ones already
handled, or ones examined and found to have nothing to extract, are left alone).

Designed to run against a large, live, unindexed record_compute_history table (no
index on `status` - filtering it is a sequential scan) without holding the full
candidate set in memory: a single producer process walks the table once, in id
order, and streams batches of candidate ids to a bounded queue; worker processes
pull batches off that queue and do the actual read/decompress/update/commit work.
Ordering by id with a strictly-advancing cursor is what guarantees every row is
visited exactly once over the life of a run, regardless of whether it's actually
fixable (a plain, non-procedure failure never gains a stdout output, so it would
match the candidate filter forever if that filter were naively requeried).
"""

import argparse
import multiprocessing
from queue import Empty

import tqdm
from sqlalchemy import select
from sqlalchemy.orm import selectinload, undefer

from qcfractal.components.record_db_models import RecordComputeHistoryORM, OutputStoreORM
from qcfractal.components.record_socket import _collect_nested_outputs, _NESTED_OUTPUT_SEPARATOR
from qcfractal.components.outputstore.utils import create_output_orm
from qcfractal.config import read_configuration
from qcfractal.db_socket.socket import SQLAlchemySocket
from qcportal.record_models import OutputTypeEnum, RecordStatusEnum

# How many pending batches may sit in the queue before the producer blocks. Bounds
# memory to roughly queue_maxsize * batch_size ids in flight, instead of the whole
# candidate set.
QUEUE_MAXSIZE = 4


def _candidates_page_stmt(after_id, batch_size):
    """
    One page of candidate history ids, ordered by id, starting strictly after `after_id`.
    """
    stmt = (
        select(RecordComputeHistoryORM.id)
        .where(RecordComputeHistoryORM.status == RecordStatusEnum.error)
        .where(RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.error))
        .where(~RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.stdout))
    )
    if after_id is not None:
        stmt = stmt.where(RecordComputeHistoryORM.id > after_id)
    return stmt.order_by(RecordComputeHistoryORM.id).limit(batch_size)


def _process_batch(session, history_ids, dry_run) -> int:
    """Examines one batch of (already paged) history ids. Returns how many were
    actually updated. Never called twice for the same id within a run."""

    stmt = (
        select(RecordComputeHistoryORM)
        .where(RecordComputeHistoryORM.id.in_(history_ids))
        .options(selectinload(RecordComputeHistoryORM.outputs).options(undefer(OutputStoreORM.data)))
    )
    histories = session.execute(stmt).scalars().all()

    n_updated = 0
    for history in histories:
        error_orm = history.outputs.get(OutputTypeEnum.error)
        if error_orm is None:
            continue

        error_dict = error_orm.get_output()
        extras = error_dict.get("extras") if isinstance(error_dict, dict) else None

        stdout_parts = _collect_nested_outputs(extras, "stdout")
        stderr_parts = _collect_nested_outputs(extras, "stderr")

        # Nothing nested to extract - this row will never gain a stdout output, but that's
        # fine: the producer's cursor has already moved past it, so it won't be revisited
        # within this run.
        if not stdout_parts and not stderr_parts:
            continue

        if not dry_run:
            if stdout_parts:
                history.outputs[OutputTypeEnum.stdout] = create_output_orm(
                    OutputTypeEnum.stdout, _NESTED_OUTPUT_SEPARATOR.join(stdout_parts)
                )
            if stderr_parts:
                history.outputs[OutputTypeEnum.stderr] = create_output_orm(
                    OutputTypeEnum.stderr, _NESTED_OUTPUT_SEPARATOR.join(stderr_parts)
                )

        n_updated += 1

    if not dry_run:
        session.commit()
    else:
        session.rollback()

    return n_updated


def producer_process(fractal_config, batch_size, nproc, task_queue):
    """
    Walks the whole table exactly once, in id order, pushing batches of candidate ids
    to task_queue. Never holds more than one page's worth of ids in memory at a time.
    """

    socket = SQLAlchemySocket(fractal_config)
    session = socket.Session()

    after_id = None
    while True:
        page = session.execute(_candidates_page_stmt(after_id, batch_size)).scalars().all()

        if not page:
            break

        task_queue.put(page)
        after_id = page[-1]

    # Tell every worker there's no more work coming.
    for _ in range(nproc):
        task_queue.put(None)


def worker_process(fractal_config, dry_run, task_queue, done_queue):
    socket = SQLAlchemySocket(fractal_config)
    session = socket.Session()

    while True:
        batch = task_queue.get()
        if batch is None:
            break

        _process_batch(session, batch, dry_run)
        done_queue.put(len(batch))


if __name__ == "__main__":

    argparser = argparse.ArgumentParser(prog="QCFractal Nested Error Stdout Backfill")
    argparser.add_argument("config", help="Path to the qcfractal configuration file")
    argparser.add_argument("--nproc", type=int, default=1, help="Number of worker processes to use")
    argparser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of history rows fetched/committed per database round trip",
    )
    argparser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write anything, just report how many history entries would be updated",
    )
    args = argparser.parse_args()

    fractal_config = read_configuration([args.config])
    nproc = max(1, args.nproc)

    task_queue = multiprocessing.Queue(maxsize=QUEUE_MAXSIZE)
    done_queue = multiprocessing.Queue()

    producer = multiprocessing.Process(
        target=producer_process, args=(fractal_config, args.batch_size, nproc, task_queue)
    )
    producer.start()

    workers = [
        multiprocessing.Process(target=worker_process, args=(fractal_config, args.dry_run, task_queue, done_queue))
        for _ in range(nproc)
    ]
    for w in workers:
        w.start()

    # We deliberately don't compute an upfront total: on a large, unindexed table that
    # would mean doing the same expensive full scan twice. Progress is reported as a
    # running count/rate instead of a percentage.
    all_procs = [producer] + workers
    with tqdm.tqdm(unit="row") as pbar:
        while any(p.is_alive() for p in all_procs):
            try:
                done_count = done_queue.get(timeout=1)
                pbar.update(done_count)
            except Empty:
                pass

    producer.join()
    [w.join() for w in workers]
