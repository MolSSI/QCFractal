"""Msgpack Base Results Phase 1

Revision ID: 8b0cd9accaf2
Revises: 1134312ad4a3
Create Date: 2019-08-11 22:30:27.613722

"""

from alembic import op
import sqlalchemy as sa

import os
import sys

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
from migration_helpers import msgpack_migrations
from qcelemental.util import msgpackext_dumps, msgpackext_loads

# revision identifiers, used by Alembic.
revision = "8b0cd9accaf2"
down_revision = "1134312ad4a3"
branch_labels = None
depends_on = None

block_size = 100
table_name = "base_result"


def transformer(old_data):
    extras = old_data["extras"]
    extras.pop("_qcfractal_tags", None)  # cleanup old tags

    return {"extras_": msgpackext_dumps(extras)}


update_columns = {"extras"}


def upgrade():
    msgpack_migrations.json_to_msgpack_table(table_name, block_size, update_columns, transformer)


def downgrade():
    msgpack_migrations.json_to_msgpack_table_dropcols(table_name, block_size, update_columns)
