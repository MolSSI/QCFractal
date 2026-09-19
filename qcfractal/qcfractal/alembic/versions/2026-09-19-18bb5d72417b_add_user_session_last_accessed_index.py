"""add user_session last_accessed index

Revision ID: 18bb5d72417b
Revises: 67f7b25de401
Create Date: 2026-09-19 12:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "18bb5d72417b"
down_revision = "67f7b25de401"
branch_labels = None
depends_on = None


def upgrade():
    # Used by the periodic cleanup of expired user sessions
    op.create_index("ix_user_session_last_accessed", "user_session", ["last_accessed"], unique=False)


def downgrade():
    op.drop_index("ix_user_session_last_accessed", table_name="user_session")
