"""Finalize migration of molecule msgpack columns

Revision ID: 67f7b25de401
Revises: 9adf25dba3bc
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

# revision identifiers, used by Alembic.
revision = "67f7b25de401"
down_revision = "9adf25dba3bc"
branch_labels = None
depends_on = None

def _msgpackext_decode(obj: Any) -> Any:
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


def upgrade():
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


def downgrade():
    raise NotImplementedError("Cannot downgrade")
