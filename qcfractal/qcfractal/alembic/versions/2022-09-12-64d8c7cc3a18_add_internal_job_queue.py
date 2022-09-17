"""Add internal job queue

Revision ID: 64d8c7cc3a18
Revises: 9c91ede09098
Create Date: 2022-09-12 16:06:24.175386

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "64d8c7cc3a18"
down_revision = "9c91ede09098"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "internal_jobs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "status",
            sa.Enum("complete", "waiting", "running", "error", "cancelled", name="internaljobstatusenum"),
            nullable=False,
        ),
        sa.Column("added_date", sa.DateTime(), nullable=False),
        sa.Column("scheduled_date", sa.DateTime(), nullable=False),
        sa.Column("started_date", sa.DateTime(), nullable=True),
        sa.Column("last_updated", sa.DateTime(), nullable=True),
        sa.Column("ended_date", sa.DateTime(), nullable=True),
        sa.Column("runner_hostname", sa.String(), nullable=True),
        sa.Column("runner_uuid", sa.String(), nullable=True),
        sa.Column("progress", sa.Integer(), nullable=False),
        sa.Column("function", sa.String(), nullable=False),
        sa.Column("kwargs", sa.JSON(), nullable=False),
        sa.Column("after_function", sa.String(), nullable=True),
        sa.Column("after_function_kwargs", sa.JSON(), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("user", sa.String(), nullable=True),
        sa.Column("unique_name", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("unique_name", name="ux_internal_jobs_unique_name"),
    )
    op.create_index("ix_internal_jobs_added_date", "internal_jobs", ["added_date"], unique=False)
    op.create_index("ix_internal_jobs_last_updated", "internal_jobs", ["last_updated"], unique=False)
    op.create_index("ix_internal_jobs_name", "internal_jobs", ["name"], unique=False)
    op.create_index("ix_internal_jobs_scheduled_date", "internal_jobs", ["scheduled_date"], unique=False)
    op.create_index("ix_internal_jobs_status", "internal_jobs", ["status"], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_internal_jobs_status", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_scheduled_date", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_name", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_last_updated", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_added_date", table_name="internal_jobs")
    op.drop_table("internal_jobs")
    # ### end Alembic commands ###