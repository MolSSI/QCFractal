"""Add internal job serial group

Revision ID: 3690c677f8d1
Revises: 5f6f804e11d3
Create Date: 2025-01-10 16:08:36.541807

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3690c677f8d1"
down_revision = "5f6f804e11d3"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("internal_jobs", sa.Column("serial_group", sa.String(), nullable=True))
    op.create_index(
        "ux_internal_jobs_status_serial_group",
        "internal_jobs",
        ["status", "serial_group"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        "ux_internal_jobs_status_serial_group",
        table_name="internal_jobs",
        postgresql_where=sa.text("status = 'running'"),
    )
    op.drop_column("internal_jobs", "serial_group")
    # ### end Alembic commands ###
