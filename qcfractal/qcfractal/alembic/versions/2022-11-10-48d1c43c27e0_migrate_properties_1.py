"""Migrate properties 1

Revision ID: 48d1c43c27e0
Revises: 3cc95f9dc02c
Create Date: 2022-11-10 10:44:17.844144

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "48d1c43c27e0"
down_revision = "3cc95f9dc02c"
branch_labels = None
depends_on = None


def upgrade():
    # New temporary JSONB columns
    # We need to slowly migrate to there
    op.add_column("base_record", sa.Column("new_extras", postgresql.JSONB, nullable=True))
    op.add_column("base_record", sa.Column("new_properties", postgresql.JSONB, nullable=True))


def downgrade():
    raise RuntimeError("CANNOT DOWNGRADE")
