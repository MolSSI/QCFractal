"""Msgpack Results Phase 2

Revision ID: 1134312ad4a3
Revises: 84c94a48e491
Create Date: 2019-08-11 17:21:43.328492

"""
from alembic import op
import sqlalchemy as sa

import os
import sys

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
from migration_helpers import msgpack_migrations

# revision identifiers, used by Alembic.
revision = "1134312ad4a3"
down_revision = "84c94a48e491"
branch_labels = None
depends_on = None

table_name = "result"
update_columns = {"return_result"}

nullable = {"return_result"}


def upgrade():
    msgpack_migrations.json_to_msgpack_table_altercolumns(table_name, update_columns, nullable_true=nullable)


def downgrade():
    raise ValueError("Cannot downgrade json to msgpack conversions")
