"""Migrate molecule msgpack columns to native postgres types

The bulk of the work here (deserializing msgpack into the temporary columns) can be done
ahead of time, against a live server, with server_admin/migrate_molecule_msgpack.py. That
script adds the same temporary columns and populates them, leaving this migration with
only the column moving to do. Anything it did not get to is handled here.

Revision ID: 67f7b25de401
Revises: 865e4be6ef5c
Create Date: 2025-03-06 16:21:00.149754

"""

from typing import Any

import msgpack
import numpy as np
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import table, column, select, update
from tqdm import tqdm

# revision identifiers, used by Alembic.
revision = "67f7b25de401"
down_revision = "865e4be6ef5c"
branch_labels = None
depends_on = None

# Temporary columns used to hold the deserialized data. These are renamed into place at the
# end of this migration, so their types must match the MoleculeORM columns.
# server_admin/migrate_molecule_msgpack.py creates these same columns - keep the two in sync.
TEMPORARY_COLUMNS = [
    ("_migrated_status", postgresql.BOOLEAN()),
    ("symbols_tmp", postgresql.ARRAY(sa.String())),
    ("geometry_tmp", postgresql.ARRAY(sa.Float())),
    ("masses_tmp", postgresql.ARRAY(sa.Float())),
    ("real_tmp", postgresql.ARRAY(sa.Boolean())),
    ("atom_labels_tmp", postgresql.ARRAY(sa.String())),
    ("atomic_numbers_tmp", postgresql.ARRAY(sa.Integer())),
    ("mass_numbers_tmp", postgresql.ARRAY(sa.Float())),
    ("fragments_tmp", postgresql.JSON()),
    ("fragment_charges_tmp", postgresql.ARRAY(sa.Float())),
    ("fragment_multiplicities_tmp", postgresql.ARRAY(sa.Float())),
]


def _msgpackext_decode(obj: Any) -> Any:
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

    if isinstance(v, np.ndarray):
        return v.ravel().tolist()

    # awkward, but things like "fragments" might be a list of np arrays
    if isinstance(v, list) and len(v) > 0 and isinstance(v[0], np.ndarray):
        return [v.tolist() for v in v]

    # Anything stored by a newer server is already plain lists/scalars
    return v


def _add_temporary_columns():
    """
    Adds the temporary columns, skipping any that server_admin/migrate_molecule_msgpack.py
    created already
    """

    bind = op.get_bind()
    existing = {c["name"] for c in sa.inspect(bind).get_columns("molecule")}

    for col_name, col_type in TEMPORARY_COLUMNS:
        if col_name not in existing:
            op.add_column("molecule", sa.Column(col_name, col_type, nullable=True))

    # Needed to find the rows left to do without scanning the whole table. Always rebuild:
    # an interrupted build leaves an invalid
    # index that CREATE INDEX IF NOT EXISTS would keep (it matches on the name) even though
    # the planner ignores it.
    op.execute("DROP INDEX IF EXISTS ix_molecule__migrated_status")
    op.execute("CREATE INDEX ix_molecule__migrated_status ON molecule (_migrated_status)")


def upgrade():
    # This migration is one long transaction - hours of it, on a large database. Both of
    # these default to 0 (no limit) in postgres, but plenty of deployments set them (per
    # database, per role, or by a managed provider), and either would kill the migration
    # part way through and roll all of the work back.
    op.execute("SET LOCAL statement_timeout = 0")

    # transaction_timeout only exists from postgres 17; setting it on anything older is an
    # error, which would abort the very migration this is meant to protect
    has_transaction_timeout = (
        op.get_bind().execute(sa.text("SELECT 1 FROM pg_settings WHERE name = 'transaction_timeout'")).scalar()
    )
    if has_transaction_timeout:
        op.execute("SET LOCAL transaction_timeout = 0")

    _add_temporary_columns()

    mol_table = table(
        "molecule",
        column("id", sa.Integer),
        column("_migrated_status", sa.Boolean),
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
        column("symbols_tmp", postgresql.ARRAY(sa.String())),
        column("geometry_tmp", postgresql.ARRAY(sa.Float())),
        column("masses_tmp", postgresql.ARRAY(sa.Float())),
        column("real_tmp", postgresql.ARRAY(sa.Boolean())),
        column("atom_labels_tmp", postgresql.ARRAY(sa.String())),
        column("atomic_numbers_tmp", postgresql.ARRAY(sa.Integer())),
        column("mass_numbers_tmp", postgresql.ARRAY(sa.Float())),
        column("fragments_tmp", postgresql.JSON()),
        column("fragment_charges_tmp", postgresql.ARRAY(sa.Float())),
        column("fragment_multiplicities_tmp", postgresql.ARRAY(sa.Float())),
    )

    bind = op.get_bind()
    session = Session(bind=bind)

    # Count how many molecules still need migrating so we can show a proper progress bar
    total_to_migrate = session.execute(
        select(sa.func.count()).select_from(mol_table).where(mol_table.c._migrated_status.is_(None))
    ).scalar_one()

    print("-"*80)
    print("Performing final migration of molecule table")
    print("Total molecules to migrate:", total_to_migrate)
    if total_to_migrate > 50000:
        print("WARNING: This migration may take a long time for large numbers of molecules (100,000+). Please be patient.")
        print("         The server is unavailable until it finishes, and it cannot be undone (restoring")
        print("         a backup is the only way back).")

    progress = tqdm(total=total_to_migrate, desc="Migrating molecules", unit=" mol")

    while True:
        results = session.execute(
            select(mol_table).where(mol_table.c._migrated_status.is_(None)).limit(1000)
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

        session.execute(update(mol_table).where(mol_table.c.id == sa.bindparam("mol_id")), all_updates)
        session.flush()

        progress.update(len(results))

    progress.close()

    print("Moving molecule columns. This may take a long time for large numbers. Please be patient.")

    # Delete old columns and move temporary columns
    op.drop_column("molecule", "symbols")
    op.drop_column("molecule", "geometry")
    op.drop_column("molecule", "masses")
    op.drop_column("molecule", "real")
    op.drop_column("molecule", "atom_labels")
    op.drop_column("molecule", "atomic_numbers")
    op.drop_column("molecule", "mass_numbers")
    op.drop_column("molecule", "fragments")
    op.drop_column("molecule", "fragment_charges")
    op.drop_column("molecule", "fragment_multiplicities")

    op.alter_column("molecule", "geometry_tmp", new_column_name="geometry", nullable=False)
    op.alter_column("molecule", "symbols_tmp", new_column_name="symbols", nullable=False)
    op.alter_column("molecule", "masses_tmp", new_column_name="masses", nullable=True)
    op.alter_column("molecule", "real_tmp", new_column_name="real", nullable=True)
    op.alter_column("molecule", "atom_labels_tmp", new_column_name="atom_labels", nullable=True)
    op.alter_column("molecule", "atomic_numbers_tmp", new_column_name="atomic_numbers", nullable=True)
    op.alter_column("molecule", "mass_numbers_tmp", new_column_name="mass_numbers", nullable=True)
    op.alter_column("molecule", "fragments_tmp", new_column_name="fragments", nullable=True)
    op.alter_column("molecule", "fragment_charges_tmp", new_column_name="fragment_charges", nullable=True)
    op.alter_column("molecule", "fragment_multiplicities_tmp", new_column_name="fragment_multiplicities", nullable=True)

    # Total multiplicity is now a float
    op.alter_column(
        "molecule", "molecular_multiplicity", existing_type=sa.INTEGER(), type_=sa.Float(), existing_nullable=True
    )

    op.drop_column("molecule", "_migrated_status")
    print("DONE")


def downgrade():
    raise NotImplementedError("Cannot downgrade")
