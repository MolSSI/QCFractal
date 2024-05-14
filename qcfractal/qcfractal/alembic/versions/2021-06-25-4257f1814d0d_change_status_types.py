"""Change status types

Revision ID: 4257f1814d0d
Revises: 88182596f844
Create Date: 2022-11-23 09:32:57.598809

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "4257f1814d0d"
down_revision = "88182596f844"
branch_labels = None
depends_on = None


def upgrade():
    # Add new values to status enum
    # Done in a separate migration because of postgresql limitations
    # ("New enum values must be committed before they can be used")
    op.execute("ALTER TYPE recordstatusenum ADD VALUE 'waiting'")
    op.execute("ALTER TYPE recordstatusenum ADD VALUE 'cancelled'")
    op.execute("ALTER TYPE recordstatusenum ADD VALUE 'deleted'")


def downgrade():
    raise RuntimeError("CANNOT DOWNGRADE")
    pass
