"""
Backfills stdout/stderr for errored records whose output was nested inside
error.extras["failed_result"] rather than surfaced as a top-level stdout/stderr output.

This affects records that errored via a procedure (eg geomeTRIC, optking) wrapping a
failing sub-computation, before the fix to RecordSocket.update_failed_task. Their
compute-history error output already contains the useful stdout/stderr - it's just
nested rather than promoted to its own output row - so this script is a pure data
backfill. It does not require an alembic migration and is safe to run multiple times:
it never modifies or replaces an existing output row, only adds missing ones.

Scope: only history rows that lack a stdout output are examined (that is the observable
symptom of the bug - pre-fix procedure failures stored neither stdout nor stderr). A
history that already has stdout is assumed to predate the bug or postdate the fix and
is left entirely alone. Rows created after the script starts are also out of scope:
they were produced by fixed code and are never candidates.

Designed to run against a large, live, unindexed record_compute_history table (no
index on `status` - filtering it is a sequential scan) without holding the full
candidate set in memory: a single producer process walks the table once, in id
order up to a high-water id captured at startup, and streams batches of candidate
ids to a bounded queue; worker processes pull batches off that queue and do the
actual read/decompress/update/commit work. Ordering by id with a strictly-advancing
cursor guarantees every row is visited exactly once per run, regardless of whether
it's actually fixable (a plain, non-procedure failure never gains a stdout output,
so it would match the candidate filter forever if that filter were naively requeried).

If any child process fails, the run aborts with a nonzero exit code. Batches are
committed atomically, so an aborted or killed run leaves no partial batches - simply
re-run the script to continue.
"""

import argparse
import multiprocessing
import sys
import traceback
from queue import Empty

import tqdm
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload, undefer

from qcfractal.components.record_db_models import RecordComputeHistoryORM, OutputStoreORM
from qcfractal.components.record_socket import _collect_nested_outputs, _join_outputs
from qcfractal.components.outputstore.utils import create_output_orm
from qcfractal.config import read_configuration
from qcfractal.db_socket.socket import SQLAlchemySocket
from qcportal.record_models import OutputTypeEnum, RecordStatusEnum

# How many pending batches may sit in the queue before the producer blocks. Bounds
# memory to roughly queue_maxsize * batch_size ids in flight, instead of the whole
# candidate set.
QUEUE_MAXSIZE = 4


def _candidates_page_stmt(after_id, max_id, batch_size):
    """
    One page of candidate history ids, ordered by id, starting strictly after `after_id`
    and never exceeding `max_id` (the high-water mark captured at startup).
    """
    stmt = (
        select(RecordComputeHistoryORM.id)
        .where(RecordComputeHistoryORM.status == RecordStatusEnum.error)
        .where(RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.error))
        .where(~RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.stdout))
        .where(RecordComputeHistoryORM.id <= max_id)
    )
    if after_id is not None:
        stmt = stmt.where(RecordComputeHistoryORM.id > after_id)
    return stmt.order_by(RecordComputeHistoryORM.id).limit(batch_size)


def _process_batch(session, history_ids, dry_run):
    """Examines one batch of (already paged) history ids. Returns (n_examined, n_updated).
    Never modifies an existing output row - only adds missing ones."""

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

        # Never overwrite an output that already exists (candidates lack stdout by
        # definition, but may already have a stderr - leave it alone)
        add_stdout = bool(stdout_parts) and OutputTypeEnum.stdout not in history.outputs
        add_stderr = bool(stderr_parts) and OutputTypeEnum.stderr not in history.outputs

        # Nothing to add - this row may match the candidate filter forever, but that's
        # fine: the producer's cursor has already moved past it for this run.
        if not add_stdout and not add_stderr:
            continue

        if not dry_run:
            if add_stdout:
                history.outputs[OutputTypeEnum.stdout] = create_output_orm(
                    OutputTypeEnum.stdout, _join_outputs(stdout_parts)
                )
            if add_stderr:
                history.outputs[OutputTypeEnum.stderr] = create_output_orm(
                    OutputTypeEnum.stderr, _join_outputs(stderr_parts)
                )

        n_updated += 1

    if not dry_run:
        session.commit()
    else:
        session.rollback()

    return len(histories), n_updated


def producer_process(fractal_config, batch_size, nproc, task_queue):
    """
    Walks the table exactly once, in id order up to the startup high-water mark, pushing
    batches of candidate ids to task_queue. Never holds more than one page's worth of ids
    in memory at a time. Always sends one sentinel per worker, even on failure, so workers
    can never block forever on the queue.
    """

    try:
        socket = SQLAlchemySocket(fractal_config)
        session = socket.Session()

        # High-water mark: bounds the scan so continuous insertion of new (post-fix,
        # never-candidate) rows on a live server can't keep the run going forever
        max_id = session.execute(select(func.max(RecordComputeHistoryORM.id))).scalar_one()

        after_id = None
        while max_id is not None:
            page = session.execute(_candidates_page_stmt(after_id, max_id, batch_size)).scalars().all()

            if not page:
                break

            task_queue.put(page)
            after_id = page[-1]
    except BaseException:
        traceback.print_exc()
        raise
    finally:
        for _ in range(nproc):
            task_queue.put(None)


def worker_process(fractal_config, dry_run, task_queue, done_queue):
    try:
        socket = SQLAlchemySocket(fractal_config)
        session = socket.Session()

        while True:
            batch = task_queue.get()
            if batch is None:
                break

            n_examined, n_updated = _process_batch(session, batch, dry_run)
            done_queue.put((n_examined, n_updated))
    except BaseException:
        traceback.print_exc()
        sys.exit(1)


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
    n_examined = 0
    n_updated = 0
    failed = False

    with tqdm.tqdm(unit="row") as pbar:
        while any(p.is_alive() for p in all_procs):
            try:
                batch_examined, batch_updated = done_queue.get(timeout=1)
                n_examined += batch_examined
                n_updated += batch_updated
                pbar.update(batch_examined)
            except Empty:
                pass

            # A dead child with a nonzero exit code means the run cannot be trusted to
            # finish - abort everything rather than hang or silently drop batches
            if any(p.exitcode not in (None, 0) for p in all_procs):
                failed = True
                for p in all_procs:
                    if p.is_alive():
                        p.terminate()
                break

    # drain any counts that arrived after the loop exited
    while True:
        try:
            batch_examined, batch_updated = done_queue.get(timeout=0.25)
            n_examined += batch_examined
            n_updated += batch_updated
        except Empty:
            break

    for p in all_procs:
        p.join()
    failed = failed or any(p.exitcode != 0 for p in all_procs)

    verb = "would be updated" if args.dry_run else "updated"
    print(f"Examined {n_examined} error compute-history entries; {n_updated} {verb}")

    if failed:
        print("ERROR: a child process failed - the run is incomplete. It is safe to re-run this script.")
        sys.exit(1)
