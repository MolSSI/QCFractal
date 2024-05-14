"""Msgpack Remaining Phase 2

Revision ID: e32b61e2516f
Revises: da7c6f141bcb
Create Date: 2019-08-12 10:13:09.694643

"""

import os
import sys

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
from migration_helpers import msgpack_migrations

# revision identifiers, used by Alembic.
revision = "e32b61e2516f"
down_revision = "da7c6f141bcb"
branch_labels = None
depends_on = None


def upgrade():
    ## Task Queue
    table_name = "task_queue"
    update_columns = {"spec"}

    nullable = set()
    msgpack_migrations.json_to_msgpack_table_altercolumns(table_name, update_columns, nullable_true=nullable)

    ## Service Queue
    table_name = "service_queue"
    update_columns = {"extra"}

    nullable = set()
    msgpack_migrations.json_to_msgpack_table_altercolumns(table_name, update_columns, nullable_true=nullable)


def downgrade():
    raise ValueError("Cannot downgrade json to msgpack conversions")
