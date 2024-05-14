"""Msgpack Results Phase 1

Revision ID: 84c94a48e491
Revises: d56ac42b9a43
Create Date: 2019-08-11 17:21:40.264688

"""

import os
import sys

import numpy as np
import sqlalchemy as sa

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
from migration_helpers import msgpack_migrations
from qcelemental.util import msgpackext_dumps

# revision identifiers, used by Alembic.
revision = "84c94a48e491"
down_revision = "d56ac42b9a43"
branch_labels = None
depends_on = None

block_size = 100
table_name = "result"


def transformer(old_data):
    arr = old_data["return_result"]
    if arr is None:
        pass
    elif old_data["driver"] == "gradient":
        arr = np.array(arr, dtype=float).reshape(-1, 3)
    elif old_data["driver"] == "hessian":
        arr = np.array(arr, dtype=float)
        arr.shape = (-1, int(arr.shape[0] ** 0.5))

    return {"return_result_": msgpackext_dumps(arr)}


update_columns = {"return_result"}


def upgrade():
    msgpack_migrations.json_to_msgpack_table(
        table_name, block_size, update_columns, transformer, read_columns={"driver": sa.String}
    )


def downgrade():
    msgpack_migrations.json_to_msgpack_table_dropcols(table_name, block_size, update_columns)
