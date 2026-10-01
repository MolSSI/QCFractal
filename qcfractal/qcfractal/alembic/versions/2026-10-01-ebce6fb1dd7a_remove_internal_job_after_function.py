"""Remove after_function from internal jobs

after_function was how periodic jobs rescheduled themselves, until repeat_delay
replaced that (e798462e0c03). Nothing has set it since, so drop the columns.

Revision ID: ebce6fb1dd7a
Revises: c4805aff43a8
Create Date: 2026-10-01 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "ebce6fb1dd7a"
down_revision = "c4805aff43a8"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("internal_jobs", "after_function_kwargs")
    op.drop_column("internal_jobs", "after_function")


def downgrade():
    # Column definitions as they were. Any values they held are not restored
    op.add_column("internal_jobs", sa.Column("after_function", sa.String(), nullable=True))
    op.add_column("internal_jobs", sa.Column("after_function_kwargs", postgresql.JSON(), nullable=True))
