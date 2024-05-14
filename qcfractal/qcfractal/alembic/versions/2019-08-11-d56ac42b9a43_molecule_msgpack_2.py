"""Msgpack Molecule Phase 2

Revision ID: d56ac42b9a43
Revises: 963822c28879
Create Date: 2019-08-11 16:17:23.856255

"""

import os
import sys

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))

from migration_helpers import msgpack_migrations

# revision identifiers, used by Alembic.
revision = "d56ac42b9a43"
down_revision = "963822c28879"
branch_labels = None
depends_on = None

table_name = "molecule"
update_columns = {"symbols", "geometry", "masses", "real", "atom_labels", "atomic_numbers", "mass_numbers", "fragments"}

nullable = update_columns.copy()
nullable -= {"symbols", "geometry"}


def upgrade():
    msgpack_migrations.json_to_msgpack_table_altercolumns(table_name, update_columns, nullable_true=nullable)


def downgrade():
    raise ValueError("Cannot downgrade json to msgpack conversions")
