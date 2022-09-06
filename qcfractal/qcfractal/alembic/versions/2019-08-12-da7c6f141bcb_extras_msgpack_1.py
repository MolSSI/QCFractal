"""Msgpack Remaining Phase 1

Revision ID: da7c6f141bcb
Revises: 05ceea11b78a
Create Date: 2019-08-12 10:12:46.478628

"""
from alembic import op
import sqlalchemy as sa

import os
import sys

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
from migration_helpers import msgpack_migrations
from qcelemental.util import msgpackext_dumps, msgpackext_loads

# revision identifiers, used by Alembic.
revision = "da7c6f141bcb"
down_revision = "05ceea11b78a"
branch_labels = None
depends_on = None

block_size = 100


def transformer(old_data):

    extras = old_data["extras"]
    extras.pop("_qcfractal_tags", None)  # cleanup old tags

    return {"extras_": msgpackext_dumps(extras)}


def upgrade():

    ## Task Queue
    table_name = "task_queue"
    update_columns = {"spec"}

    def transformer(old_data):

        spec = old_data["spec"]

        return {"spec_": msgpackext_dumps(spec)}

    msgpack_migrations.json_to_msgpack_table(table_name, block_size, update_columns, transformer)

    ## Service Queue
    table_name = "service_queue"
    update_columns = {"extra"}

    def transformer(old_data):

        spec = old_data["extra"]

        return {"extra_": msgpackext_dumps(spec)}

    msgpack_migrations.json_to_msgpack_table(table_name, block_size, update_columns, transformer, {})


def downgrade():

    ## Task Queue
    table_name = "task_queue"
    update_columns = {"spec"}
    msgpack_migrations.json_to_msgpack_table_dropcols(table_name, block_size, update_columns)

    ## Service Queue
    table_name = "service_queue"
    update_columns = {"extra"}
    msgpack_migrations.json_to_msgpack_table_dropcols(table_name, block_size, update_columns)
