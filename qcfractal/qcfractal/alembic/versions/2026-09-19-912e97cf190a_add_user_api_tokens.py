"""add user api tokens

Revision ID: 912e97cf190a
Revises: e6008e11850d
Create Date: 2026-09-19 15:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "912e97cf190a"
down_revision = "e6008e11850d"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "user_api_token",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("token_hash", sa.String(), nullable=False),
        sa.Column("token_prefix", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("scope", sa.String(), server_default="unlimited", nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="cascade"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_hash", name="ux_user_api_token_token_hash"),
        sa.UniqueConstraint("user_id", "name", name="ux_user_api_token_user_id_name"),
    )
    op.create_index("ix_user_api_token_user_id", "user_api_token", ["user_id"], unique=False)


def downgrade():
    op.drop_index("ix_user_api_token_user_id", table_name="user_api_token")
    op.drop_table("user_api_token")
