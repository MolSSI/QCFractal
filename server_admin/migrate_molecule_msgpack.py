import argparse
import multiprocessing
from queue import Empty

import msgpack
import numpy as np
import tqdm
from sqlalchemy import text, Integer, Boolean, Float, String, bindparam
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import table, column, select, update

from qcfractal.config import read_configuration
from qcfractal.db_socket.socket import SQLAlchemySocket


def _msgpackext_decode(obj):
    if b"_nd_" in obj:
        arr = np.frombuffer(obj[b"data"], dtype=obj[b"dtype"])
        if b"shape" in obj:
            arr.shape = obj[b"shape"]

        return arr

    return obj


def deserialize_msgpackext(value):
    if value is None:
        return None

    v = msgpack.loads(value, object_hook=_msgpackext_decode, raw=False)

    if isinstance(v, np.ndarray):
        return v.tolist()

    # awkward, but things like "fragments" might be a list of np arrays
    if isinstance(v, list) and isinstance(v[0], np.ndarray):
        return [v.tolist() for v in v]

    return v



def migration_process(fractal_config, done_queue):

    socket = SQLAlchemySocket(fractal_config)
    session = socket.Session()

    mol_table = table(
        "molecule",
        column("id", Integer),
        column("_migrated_status", Boolean),
        column("symbols", postgresql.BYTEA()),
        column("geometry", postgresql.BYTEA()),
        column("masses", postgresql.BYTEA()),
        column("real", postgresql.BYTEA()),
        column("atom_labels", postgresql.BYTEA()),
        column("atomic_numbers", postgresql.BYTEA()),
        column("mass_numbers", postgresql.BYTEA()),
        column("fragments", postgresql.BYTEA()),
        column("fragment_charges", postgresql.JSON()),
        column("fragment_multiplicities", postgresql.JSON()),
        column("symbols_tmp", postgresql.ARRAY(String())),
        column("geometry_tmp", postgresql.ARRAY(Float())),
        column("masses_tmp", postgresql.ARRAY(Float())),
        column("real_tmp", postgresql.ARRAY(Boolean())),
        column("atom_labels_tmp", postgresql.ARRAY(String())),
        column("atomic_numbers_tmp", postgresql.ARRAY(Integer())),
        column("mass_numbers_tmp", postgresql.ARRAY(Float())),
        column("fragments_tmp", postgresql.JSON()),
        column("fragment_charges_tmp", postgresql.ARRAY(Float())),
        column("fragment_multiplicities_tmp", postgresql.ARRAY(Float())),
    )

    while True:
        results = session.execute(
            select(mol_table).where(mol_table.c._migrated_status.is_(None)).with_for_update(skip_locked=True).limit(250)
        ).fetchall()

        if not results:
            break

        all_updates = []
        for mol in results:
            updates = {
                "mol_id": mol.id,
                "symbols_tmp": deserialize_msgpackext(mol.symbols),
                "geometry_tmp": deserialize_msgpackext(mol.geometry),
                "masses_tmp": deserialize_msgpackext(mol.masses),
                "real_tmp": deserialize_msgpackext(mol.real),
                "atom_labels_tmp": deserialize_msgpackext(mol.atom_labels),
                "atomic_numbers_tmp": deserialize_msgpackext(mol.atomic_numbers),
                "mass_numbers_tmp": deserialize_msgpackext(mol.mass_numbers),
                "fragments_tmp": deserialize_msgpackext(mol.fragments),
                "fragment_charges_tmp": mol.fragment_charges,  # simple change from JSON to ARRAY
                "fragment_multiplicities_tmp": mol.fragment_multiplicities,  # simple change from JSON to ARRAY
                "_migrated_status": True,
            }

            all_updates.append(updates)

        session.execute(update(mol_table).where(mol_table.c.id == bindparam("mol_id")), all_updates)
        session.commit()

        done_queue.put(len(all_updates))


if __name__ == "__main__":

    argparser = argparse.ArgumentParser(prog="QCFractal Molecule Msgpack Migrator")
    argparser.add_argument("config", help="Path to the qcfractal configuration file")
    argparser.add_argument(
        "--nproc", type=int, default=1, help="Number of processes to use"
    )
    args = argparser.parse_args()

    fractal_config = read_configuration([args.config])
    socket = SQLAlchemySocket(fractal_config)

    session = socket.Session()

    # How many outputs are there, and how many need to be done
    stmt = text("SELECT count(id) FROM molecule WHERE molecule._migrated_status IS NULL")
    need_migrating = session.execute(stmt).scalar_one()

    stmt = text("SELECT count(id) FROM molecule")
    total_molecules = session.execute(stmt).scalar_one()

    print(f"Have {total_molecules} molecules, {need_migrating} need migrating (approx)")

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
