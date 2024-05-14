"""Msgpack Molecule Phase 1

Revision ID: 963822c28879
Revises: 4bb79efa9855
Create Date: 2019-08-10 17:41:15.520300

"""

import os
import sys

import numpy as np

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))

from migration_helpers import msgpack_migrations
from qcelemental.util import msgpackext_dumps

# revision identifiers, used by Alembic.
revision = "963822c28879"
down_revision = "4bb79efa9855"
branch_labels = None
depends_on = None

block_size = 100
table_name = "molecule"

converters = {
    "symbols": lambda arr: np.array(arr, dtype=str),
    "geometry": lambda arr: np.array(arr, dtype=float),
    "masses": lambda arr: np.array(arr, dtype=float),
    "real": lambda arr: np.array(arr, dtype=bool),
    "atom_labels": lambda arr: np.array(arr, dtype=str),
    "atomic_numbers": lambda arr: np.array(arr, dtype=np.int16),
    "mass_numbers": lambda arr: np.array(arr, dtype=np.int16),
    "fragments": lambda list_arr: [np.array(x, dtype=np.int32) for x in list_arr],
}


def transformer(old_data):
    row = {}
    for k, v in old_data.items():
        if k == "id":
            continue
        d = msgpackext_dumps(converters[k](v))
        row[k + "_"] = d

    return row


def upgrade():
    msgpack_migrations.json_to_msgpack_table(table_name, block_size, converters.keys(), transformer)


def downgrade():
    msgpack_migrations.json_to_msgpack_table_dropcols(table_name, block_size, update_columns)
