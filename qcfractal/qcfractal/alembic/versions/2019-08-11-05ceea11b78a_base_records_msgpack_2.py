"""Msgpack Base Results Phase 2

Revision ID: 05ceea11b78a
Revises: 8b0cd9accaf2
Create Date: 2019-08-11 22:30:51.453746

"""

import os
import sys

sys.path.insert(1, os.path.dirname(os.path.abspath(__file__)))
from migration_helpers import msgpack_migrations

# revision identifiers, used by Alembic.
revision = "05ceea11b78a"
down_revision = "8b0cd9accaf2"
branch_labels = None
depends_on = None

table_name = "base_result"
update_columns = {"extras"}

nullable = {"extras"}


def upgrade():
    msgpack_migrations.json_to_msgpack_table_altercolumns(table_name, update_columns, nullable_true=nullable)


def downgrade():
    raise ValueError("Cannot downgrade json to msgpack conversions")
