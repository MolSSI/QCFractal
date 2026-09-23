"""
Backfills stdout/stderr for errored records whose output was nested inside
error.extras["failed_result"] rather than surfaced as a top-level stdout/stderr output.

This affects records that errored via a procedure (eg geomeTRIC, optking) wrapping a
failing sub-computation, before the fix to RecordSocket.update_failed_task. Their
compute-history error output already contains the useful stdout/stderr - it's just
nested rather than promoted to its own output row - so this script is a pure data
backfill. It does not require an alembic migration and is safe to run multiple times
(only history rows still missing a stdout output are touched).
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


def _candidates_stmt():
    # history rows that errored, have an error output (so there's something to mine),
    # but no stdout output yet (so we don't touch rows the normal code path already handled)
    return (
        select(RecordComputeHistoryORM.id)
        .where(RecordComputeHistoryORM.status == RecordStatusEnum.error)
        .where(RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.error))
        .where(~RecordComputeHistoryORM.outputs.any(OutputStoreORM.output_type == OutputTypeEnum.stdout))
    )


def migration_process(fractal_config, dry_run, done_queue):

    socket = SQLAlchemySocket(fractal_config)
    session = socket.Session()

    while True:
        stmt = _candidates_stmt().limit(50).with_for_update(skip_locked=True)
        history_ids = session.execute(stmt).scalars().all()

        if len(history_ids) == 0:
            break

        stmt = (
            select(RecordComputeHistoryORM)
            .where(RecordComputeHistoryORM.id.in_(history_ids))
            .options(selectinload(RecordComputeHistoryORM.outputs).options(undefer(OutputStoreORM.data)))
        )
        histories = session.execute(stmt).scalars().all()

        updated = 0
        for history in histories:
            error_orm = history.outputs.get(OutputTypeEnum.error)
            if error_orm is None:
                continue

            error_dict = error_orm.get_output()
            extras = error_dict.get("extras") if isinstance(error_dict, dict) else None

            stdout_parts = _collect_nested_outputs(extras, "stdout")
            stderr_parts = _collect_nested_outputs(extras, "stderr")

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

            updated += 1

        if not dry_run:
            session.commit()
        else:
            session.rollback()

        done_queue.put(len(histories))


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

    stmt = select(func.count()).select_from(_candidates_stmt().subquery())
    need_migrating = session.execute(stmt).scalar_one()

    print(f"{need_migrating} compute-history entries are candidates for a stdout/stderr backfill (approx)")

    if need_migrating == 0:
        raise SystemExit(0)

    proc_pool = []
    done_queue = multiprocessing.Queue()

    for _ in range(args.nproc):
        proc = multiprocessing.Process(target=migration_process, args=(fractal_config, args.dry_run, done_queue))
        proc.start()
        proc_pool.append(proc)

    with tqdm.tqdm(total=need_migrating) as pbar:
        while any(x.is_alive() for x in proc_pool):
            try:
                migrated_count = done_queue.get(timeout=1)
                pbar.update(migrated_count)
            except Empty:
                pass

    [p.join() for p in proc_pool]
