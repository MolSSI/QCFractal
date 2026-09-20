"""hash user session keys

Revision ID: e6008e11850d
Revises: 18bb5d72417b
Create Date: 2026-09-19 12:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "e6008e11850d"
down_revision = "18bb5d72417b"
branch_labels = None
depends_on = None


def upgrade():
    # The session key is a bearer credential, so only its hash is stored from now on. Existing
    # rows hold the key itself, and hashing them in place keeps everyone logged in
    op.alter_column("user_session", "session_key", new_column_name="session_key_hash")
    op.execute("UPDATE user_session SET session_key_hash = encode(sha256(convert_to(session_key_hash, 'UTF8')), 'hex')")
    op.execute(
        "ALTER TABLE user_session RENAME CONSTRAINT ux_user_session_session_key TO ux_user_session_session_key_hash"
    )


def downgrade():
    # A key cannot be recovered from its hash, so the sessions have to go. Users log in again
    op.execute("DELETE FROM user_session")
    op.execute(
        "ALTER TABLE user_session RENAME CONSTRAINT ux_user_session_session_key_hash TO ux_user_session_session_key"
    )
    op.alter_column("user_session", "session_key_hash", new_column_name="session_key")
