"""Msgpack Molecule Phase 1

Revision ID: 963822c28879
Revises: 4bb79efa9855
Create Date: 2019-08-10 17:41:15.520300

"""
from alembic import op
import sqlalchemy as sa
import numpy as np

import os
import sys
sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))

from migration_helpers import msgpack_migrations

# revision identifiers, used by Alembic.
revision = '963822c28879'
down_revision = '4bb79efa9855'
branch_labels = None
depends_on = None

block_size = 100
table_name = "molecule"


def _func(list_arr):
    return [np.array(x, dtype=np.int32) for x in list_arr]


update_columns = {
    "symbols": lambda arr: np.array(arr, dtype=str),
    "geometry": lambda arr: np.array(arr, dtype=float),
    "masses": lambda arr: np.array(arr, dtype=float),
    "real": lambda arr: np.array(arr, dtype=bool),
    "atom_labels": lambda arr: np.array(arr, dtype=str),
    "atomic_numbers": lambda arr: np.array(arr, dtype=np.int16),
    "mass_numbers": lambda arr: np.array(arr, dtype=np.int16),
    "fragments": lambda list_arr: [np.array(x, dtype=np.int32) for x in list_arr],
}


def upgrade():
    msgpack_migrations.json_to_msgpack_table(table_name, block_size, update_columns)


def downgrade():
    msgpack_migrations.json_to_msgpack_table_dropcols(table_name, block_size, update_columns)
