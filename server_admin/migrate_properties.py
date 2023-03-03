import argparse
import multiprocessing
from queue import Empty

import numpy as np
import tqdm
from sqlalchemy import func, select
from sqlalchemy.orm import load_only

from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.config import read_configuration
from qcfractal.db_socket.socket import SQLAlchemySocket


def convert_numpy(obj):
    if isinstance(obj, dict):
        return {k: convert_numpy(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [convert_numpy(v) for v in obj]
    elif isinstance(obj, np.ndarray):
        if obj.shape:
            return obj.ravel().tolist()
        else:
            return obj.tolist()
    else:
        return obj


def migration_process(fractal_config, done_queue):

    socket = SQLAlchemySocket(fractal_config)
    session = socket.Session()

    while True:
        stmt = select(BaseRecordORM)
        stmt = stmt.options(
            load_only(
                BaseRecordORM.extras,
                BaseRecordORM.new_extras,
                BaseRecordORM.new_properties,
            )
        )
        stmt = stmt.where(
            BaseRecordORM.new_extras.is_(None), BaseRecordORM.new_properties.is_(None)
        )
        stmt = stmt.with_for_update(skip_locked=True)
        stmt = stmt.limit(50)

        base_records = session.execute(stmt).scalars().all()

        if len(base_records) == 0:
            break

        for rec in base_records:
            # Find singlepoint rec if it exists
            new_properties = {}
            new_extras = {}

            # Move return_result and properties from singlepoint into base record propertiesAnd return result
            if rec.record_type == "singlepoint":
                if rec.return_result is not None:
                    new_properties["return_result"] = convert_numpy(rec.return_result)
                if rec.properties is not None:
                    new_properties.update(rec.properties)

                rec.return_result = None
                rec.properties = None

            # convert any numpy objects in extras
            if rec.extras:
                new_extras = convert_numpy(rec.extras)

            # Add qcvars from extras (and remove from extras)
            if new_extras:
                qcvars = new_extras.pop("qcvars", {})
                if qcvars:
                    new_properties.update({k.lower(): v for k, v in qcvars.items()})

            rec.extras = None
            rec.new_extras = new_extras
            rec.new_properties = new_properties

        session.commit()
        done_queue.put(len(base_records))


if __name__ == "__main__":

    argparser = argparse.ArgumentParser(prog="QCFractal OutputStore Migrator")
    argparser.add_argument("config", help="Path to the qcfractal configuration file")
    argparser.add_argument(
        "--nproc", type=int, default=1, help="Number of processes to use"
    )
    args = argparser.parse_args()

    fractal_config = read_configuration([args.config])
    socket = SQLAlchemySocket(fractal_config)

    session = socket.Session()

    # How many outputs are there, and how many need to be done
    stmt = select(func.count(BaseRecordORM.id))
    stmt = stmt.where(
        BaseRecordORM.new_extras.is_(None), BaseRecordORM.new_properties.is_(None)
    )
    need_migrating = session.execute(stmt).scalar_one()

    stmt = select(func.count(BaseRecordORM.id))
    total_records = session.execute(stmt).scalar_one()

    print(
        f"Have {total_records} records entries, {need_migrating} need migrating (approx)"
    )

    # Set up the process pool
    proc_pool = []
    done_queue = multiprocessing.Queue()

    for _ in range(args.nproc):
        proc = multiprocessing.Process(
            target=migration_process, args=(fractal_config, done_queue)
        )
        proc.start()
        proc_pool.append(proc)

    with tqdm.tqdm(total=need_migrating) as pbar:
        while any(x.is_alive() for x in proc_pool):
            try:
                migrated_count = done_queue.get(timeout=1)
                pbar.update(migrated_count)
            except Empty as e:  # empty queue is ok
                pass

    [p.join() for p in proc_pool]
