import argparse
import json
import lzma
import multiprocessing
from queue import Empty

import tqdm
import zstandard
from sqlalchemy import select, func

from qcfractal.components.record_db_models import OutputStoreORM
from qcfractal.config import read_configuration
from qcfractal.db_socket.socket import SQLAlchemySocket
from qcportal.compression import CompressionEnum, compress


def decompress_old_string(
    compressed_data: bytes, compression_type: CompressionEnum
) -> str:
    if compression_type == CompressionEnum.none:
        b = compressed_data
    elif compression_type == CompressionEnum.lzma:
        b = lzma.decompress(compressed_data)
    elif compression_type == CompressionEnum.zstd:
        b = zstandard.decompress(compressed_data)
    else:
        raise TypeError(f"Unknown compression type: {compression_type}")
    return b.decode()


def migration_process(fractal_config, done_queue):

    socket = SQLAlchemySocket(fractal_config)
    session = socket.Session()

    while True:
        # Do the records in batches
        stmt = (
            select(OutputStoreORM)
            .where(OutputStoreORM.data.is_(None))
            .limit(10)
            .with_for_update(skip_locked=True)
        )
        outputs = session.execute(stmt).scalars().all()

        if len(outputs) == 0:
            break

        for orm in outputs:
            val = orm.value
            old_d = orm.old_data

            if old_d is not None:
                # If stored the old way, convert to the new way
                # decompress to string first, then see if it's actually json
                # then recompress server side
                dstr = decompress_old_string(old_d, orm.compression_type)
                if dstr[0] == "{" and "error" in dstr:
                    cdata, ctype, clevel = compress(
                        json.loads(dstr), CompressionEnum.zstd
                    )
                else:
                    cdata, ctype, clevel = compress(dstr, CompressionEnum.zstd)
            else:
                # Compress what was in 'value'
                cdata, ctype, clevel = compress(val, CompressionEnum.zstd)

            orm.data = cdata
            orm.compression_type = ctype
            orm.compression_level = clevel
            orm.old_data = None
            orm.value = None

        session.commit()
        done_queue.put(len(outputs))


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
    stmt = select(func.count(OutputStoreORM.id)).where(OutputStoreORM.data.is_(None))
    need_migrating = session.execute(stmt).scalar_one()

    stmt = select(func.count(OutputStoreORM.id))
    total_outputs = session.execute(stmt).scalar_one()

    print(
        f"Have {total_outputs} output entries, {need_migrating} need migrating (approx)"
    )

    # Set up the process poosB
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
