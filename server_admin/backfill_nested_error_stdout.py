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
"""

import argparse
import multiprocessing
from queue import Empty

import tqdm
from sqlalchemy import select, func
from sqlalchemy.orm import selectinload, undefer

from qcfractal.components.record_db_models import RecordComputeHistoryORM, OutputStoreORM
from qcfractal.components.record_socket import _collect_nested_outputs, _NESTED_OUTPUT_SEPARATOR
from qcfractal.components.outputstore.utils import create_output_orm
from qcfractal.config import read_configuration
from qcfractal.db_socket.socket import SQLAlchemySocket
from qcportal.record_models import OutputTypeEnum, RecordStatusEnum

BATCH_SIZE = 50


def _candidates_stmt():
    # history rows that errored, have an error output (so there's something to mine),
    # but no stdout output yet (so we don't touch rows the normal code path already handled).
    #
    # NOTE: this is only a coarse pre-filter. Not every row matching it actually has a
    # nested failed_result to extract (eg a plain program failure, as opposed to a
    # procedure like geomeTRIC/optking wrapping one) - those rows will never gain a
    # stdout output and so would ALWAYS match this filter again. Callers must not
    # requery this filter in a loop expecting it to eventually come back empty;
    # instead, the candidate id list is snapshotted once (see __main__) and each
    # id is visited exactly one time.
    return (
        select(RecordComputeHistoryORM.id)
        .where(RecordComputeHistoryORM.status == RecordStatusEnum.error)
        .where(RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.error))
        .where(~RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.stdout))
    )


def _process_batch(session, history_ids, dry_run) -> int:
    """Examines one batch of (already snapshotted) history ids. Returns how many were
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
        # fine: we snapshotted the candidate list up front, so we simply won't revisit it.
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


def worker_process(fractal_config, history_ids, dry_run, done_queue):
    """Processes a fixed, disjoint slice of history ids exactly once each."""

    socket = SQLAlchemySocket(fractal_config)
    session = socket.Session()

    for i in range(0, len(history_ids), BATCH_SIZE):
        chunk = history_ids[i : i + BATCH_SIZE]
        _process_batch(session, chunk, dry_run)
        done_queue.put(len(chunk))


if __name__ == "__main__":

    argparser = argparse.ArgumentParser(prog="QCFractal Nested Error Stdout Backfill")
    argparser.add_argument("config", help="Path to the qcfractal configuration file")
    argparser.add_argument("--nproc", type=int, default=1, help="Number of processes to use")
    argparser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write anything, just report how many history entries would be updated",
    )
    args = argparser.parse_args()

    fractal_config = read_configuration([args.config])
    socket = SQLAlchemySocket(fractal_config)

    session = socket.Session()

    # Snapshot the full candidate list once. Every id in this list is visited exactly one
    # time below, regardless of whether it turns out to actually be fixable - that's what
    # guarantees this script terminates.
    all_ids = session.execute(_candidates_stmt()).scalars().all()

    print(f"{len(all_ids)} compute-history entries are candidates for a stdout/stderr backfill (exact)")

    if len(all_ids) == 0:
        raise SystemExit(0)

    nproc = max(1, args.nproc)
    id_chunks = [all_ids[i::nproc] for i in range(nproc)]
    id_chunks = [c for c in id_chunks if c]

    proc_pool = []
    done_queue = multiprocessing.Queue()

    for chunk in id_chunks:
        proc = multiprocessing.Process(
            target=worker_process, args=(fractal_config, chunk, args.dry_run, done_queue)
        )
        proc.start()
        proc_pool.append(proc)

    with tqdm.tqdm(total=len(all_ids)) as pbar:
        while any(x.is_alive() for x in proc_pool):
            try:
                done_count = done_queue.get(timeout=1)
                pbar.update(done_count)
            except Empty:
                pass

    [p.join() for p in proc_pool]
