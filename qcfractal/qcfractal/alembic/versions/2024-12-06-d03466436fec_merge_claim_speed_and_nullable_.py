"""Merge claim_speed and nullable properties branches

Revision ID: d03466436fec
Revises: 12e2ba353ee6, 03c96181c90f
Create Date: 2024-12-06 11:32:14.436022

"""

# revision identifiers, used by Alembic.
revision = "d03466436fec"
down_revision = ("12e2ba353ee6", "03c96181c90f")
branch_labels = None
depends_on = None


def upgrade():
    # Nothing to do here. Branches were completely compatible
    pass


def downgrade():
    # Nothing to do here. Branches were completely compatible
    pass
