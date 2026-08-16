"""
Migrates the msgpack columns of the molecule table to native postgres types.

This script is meant to be run against a *live, running* server before upgrading it. It
creates its own temporary columns and does not touch alembic_version, so the currently
running server is unaffected (it does not know about the temporary columns, so they never
show up in anything it serves).

Once this has finished, `qcfractal-server upgrade-db` only has the (fast) column moving
left to do. Running it is entirely optional - if you skip it, the migration does the same
work itself, but with the server down.

This script is safe to interrupt and re-run, and may be run repeatedly. Only one copy may
run at a time (it takes an advisory lock).
"""

import argparse
import multiprocessing
import os
import signal
import sys
from queue import Empty

import msgpack
import numpy as np
import tqdm
from sqlalchemy import create_engine, text, Integer, Boolean, Float, String, bindparam
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import table, column, select, update

from qcfractal.config import read_configuration

# Arbitrary fixed key, used so that two copies of this script can detect each other
ADVISORY_LOCK_KEY = 8574103924

# ADD COLUMN only needs a moment, but it needs ACCESS EXCLUSIVE to get it. If some other
# query is holding a read lock on the molecule table, waiting for that lock would queue us
# *ahead* of every new reader and stall the running server for as long as that query takes.
# Give up quickly instead and let the operator retry when things are quieter.
DDL_LOCK_TIMEOUT = "5s"

MIGRATED_STATUS_INDEX = "ix_molecule__migrated_status"

# The temporary columns this script adds. These must match what the
# "finalize_migration_of_molecule_msgpack" alembic revision would create, since whichever
# of the two runs first wins, and these columns are later renamed into place.
TEMPORARY_COLUMNS = [
    ("_migrated_status", "BOOLEAN"),
    ("symbols_tmp", "VARCHAR[]"),
    ("geometry_tmp", "DOUBLE PRECISION[]"),
    ("masses_tmp", "DOUBLE PRECISION[]"),
    ("real_tmp", "BOOLEAN[]"),
    ("atom_labels_tmp", "VARCHAR[]"),
    ("atomic_numbers_tmp", "INTEGER[]"),
    ("mass_numbers_tmp", "DOUBLE PRECISION[]"),
    ("fragments_tmp", "JSON"),
    ("fragment_charges_tmp", "DOUBLE PRECISION[]"),
    ("fragment_multiplicities_tmp", "DOUBLE PRECISION[]"),
]


def _msgpackext_decode(obj):
    if b"_nd_" in obj:
        # Deliberately not restoring the shape. The data was written in C order and the
        # only multi-dimensional value is geometry, which is stored flattened - so the
        # caller would just ravel() it straight back to what frombuffer already returns.
        # (Assigning to .shape is also deprecated as of numpy 2.5)
        return np.frombuffer(obj[b"data"], dtype=obj[b"dtype"])

    return obj


def deserialize_msgpackext(value):
    if value is None:
        return None

    v = msgpack.loads(value, object_hook=_msgpackext_decode, raw=False)

    # Flattened list
    if isinstance(v, np.ndarray):
        return v.ravel().tolist()

    # awkward, but things like "fragments" might be a list of np arrays
    if isinstance(v, list) and len(v) > 0 and isinstance(v[0], np.ndarray):
        return [v.tolist() for v in v]

    # Anything stored by a newer server is already plain lists/scalars
    return v


def add_temporary_columns(uri):
    """
    Adds the temporary columns used by this migration, if they don't exist already

    This does not change alembic_version, and is invisible to a running server.
    """

    engine = create_engine(uri)

    print("Adding temporary columns. Please wait...")
    with engine.begin() as conn:
        conn.execute(text(f"SET LOCAL lock_timeout = '{DDL_LOCK_TIMEOUT}'"))
        for col_name, col_type in TEMPORARY_COLUMNS:
            print("   Adding column", col_name)
            conn.execute(text(f"ALTER TABLE molecule ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))

    # Without this index, finding the rows left to do is a sequential scan of the whole
    # table. Build it CONCURRENTLY - a plain CREATE INDEX takes a SHARE lock, which blocks
    # everything trying to write molecules for as long as the build takes. CONCURRENTLY
    # cannot run inside a transaction, hence the autocommit connection.
    #
    # Always rebuild rather than reusing what is there. An interrupted CONCURRENTLY build
    # leaves an invalid index behind, and CREATE INDEX IF NOT EXISTS would keep it (it
    # matches on the name) even though the planner ignores it. Rebuilding is cheap.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"DROP INDEX CONCURRENTLY IF EXISTS {MIGRATED_STATUS_INDEX}"))
        conn.execute(text(f"CREATE INDEX CONCURRENTLY {MIGRATED_STATUS_INDEX} ON molecule (_migrated_status)"))

    engine.dispose()


def migration_process(uri, done_queue):

    # If the parent is killed outright (kill -9), nothing it does can clean us up. Notice
    # that we have been reparented and stop, rather than carrying on unsupervised.
    parent_pid = os.getppid()

    engine = create_engine(uri)
    session = sessionmaker(bind=engine)()

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

    try:
        while True:
            if os.getppid() != parent_pid:
                break

            results = session.execute(
                select(mol_table)
                .where(mol_table.c._migrated_status.is_(None))
                .with_for_update(skip_locked=True)
                .limit(250)
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
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":

    argparser = argparse.ArgumentParser(prog="QCFractal Molecule Msgpack Migrator")
    argparser.add_argument("config", help="Path to the qcfractal configuration file")
    argparser.add_argument(
        "--nproc", type=int, default=1, help="Number of processes to use"
    )
    args = argparser.parse_args()

    # Turn a `kill` into a normal exit, so the cleanup below actually runs
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(1))

    fractal_config = read_configuration([args.config])
    uri = fractal_config.database.sqlalchemy_url

    # Nothing to do if the database has already been fully upgraded
    engine = create_engine(uri)
    with engine.begin() as conn:
        already_done = conn.execute(
            text(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_name = 'molecule' AND column_name = 'geometry' AND data_type = 'ARRAY'"
            )
        ).scalar_one()

    if already_done:
        print("The molecule table has already been migrated - nothing to do")
        sys.exit(0)

    # Only one copy of this script may run at a time. Two would fight over the ADD COLUMN
    # below, and while one of them waits for that lock it blocks every reader of the
    # molecule table. Autocommit, so this connection never sits idle in a transaction
    # (which would itself hold a lock on molecule for the whole run).
    lock_conn = engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    got_lock = lock_conn.execute(text("SELECT pg_try_advisory_lock(:key)"), {"key": ADVISORY_LOCK_KEY}).scalar_one()
    if not got_lock:
        print("Another copy of this script is already running - exiting")
        sys.exit(1)

    # A no-op if a previous run of this script already added them
    add_temporary_columns(uri)

    # How many molecules are there, and how many need to be done. Deliberately not holding
    # a session open across the run - see the comment on the advisory lock above.
    with engine.begin() as conn:
        need_migrating = conn.execute(
            text("SELECT count(id) FROM molecule WHERE molecule._migrated_status IS NULL")
        ).scalar_one()
        total_molecules = conn.execute(text("SELECT count(id) FROM molecule")).scalar_one()

    print(f"Have {total_molecules} molecules, {need_migrating} need migrating (approx)")

    # Set up the process pool. daemon=True so the workers cannot outlive us - orphaned
    # workers would keep writing to the database with nothing supervising them.
    proc_pool = []
    done_queue = multiprocessing.Queue()

    try:
        for _ in range(args.nproc):
            proc = multiprocessing.Process(
                target=migration_process, args=(uri, done_queue), daemon=True
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
    finally:
        # Ctrl-C, an exception here, anything - do not leave workers behind
        for p in proc_pool:
            if p.is_alive():
                p.terminate()
        for p in proc_pool:
            p.join(timeout=10)

    # A worker that died leaves molecules unmigrated. Don't exit zero - the caller may be
    # a script that goes on to run `qcfractal-server upgrade-db`
    failed = [p.exitcode for p in proc_pool if p.exitcode != 0]
    if failed:
        print(f"\nERROR: {len(failed)} of {len(proc_pool)} workers exited abnormally (exit codes {failed})")
        print("See the errors above. Not all molecules have been migrated; re-run this script.")
        sys.exit(1)

    with engine.begin() as conn:
        remaining = conn.execute(
            text("SELECT count(id) FROM molecule WHERE molecule._migrated_status IS NULL")
        ).scalar_one()

    lock_conn.close()  # releases the advisory lock
    engine.dispose()

    if remaining:
        # Rows added by the running server after the workers finished. Harmless - the
        # alembic migration will handle whatever is left
        print(f"Done. {remaining} molecules were added while this was running and are left for the upgrade")
    else:
        print("Done. All molecules migrated")
