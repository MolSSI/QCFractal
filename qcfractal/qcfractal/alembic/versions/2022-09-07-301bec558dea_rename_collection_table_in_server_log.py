"""Rename collection table in server log

Revision ID: 301bec558dea
Revises: 48f4d60735cf
Create Date: 2022-09-07 10:05:49.576236

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "301bec558dea"
down_revision = "48f4d60735cf"
branch_labels = None
depends_on = None


def upgrade():
    # Rename entries in the server stats log
    # (for simplicity, just do a text search and replace)
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'collection', 'base_dataset')::json"
        )
    )
    pass


def downgrade():
    pass
