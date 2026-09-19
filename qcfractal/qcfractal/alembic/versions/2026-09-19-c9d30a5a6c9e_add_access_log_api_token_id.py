"""add access log api token id

Revision ID: c9d30a5a6c9e
Revises: 912e97cf190a
Create Date: 2026-09-19 16:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c9d30a5a6c9e"
down_revision = "912e97cf190a"
branch_labels = None
depends_on = None


def upgrade():
    # Plain integer, not a foreign key: revoking a token must not be blocked by (or erase) its
    # access history
    op.add_column("access_log", sa.Column("api_token_id", sa.Integer(), nullable=True))


def downgrade():
    op.drop_column("access_log", "api_token_id")
