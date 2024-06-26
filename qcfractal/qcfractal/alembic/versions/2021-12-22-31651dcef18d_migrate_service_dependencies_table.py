"""migrate service dependencies table

Revision ID: 31651dcef18d
Revises: 7a60e93aed72
Create Date: 2021-12-22 15:29:29.396561

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "31651dcef18d"
down_revision = "7a60e93aed72"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.rename_table("service_queue_tasks", "service_dependencies")

    op.alter_column(
        "service_dependencies",
        "extras",
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        nullable=False,
    )
    op.create_unique_constraint(
        "ux_service_dependencies", "service_dependencies", ["service_id", "record_id", "extras"]
    )

    op.execute(sa.text("ALTER SEQUENCE service_queue_tasks_id_seq RENAME TO service_dependencies_id_seq"))
    op.execute(sa.text("ALTER INDEX service_queue_tasks_pkey RENAME TO service_dependencies_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE service_dependencies RENAME CONSTRAINT service_queue_tasks_record_id_fkey TO service_dependencies_record_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE service_dependencies RENAME CONSTRAINT service_queue_tasks_service_id_fkey TO service_dependencies_service_id_fkey"
        )
    )

    # ### end Alembic commands ###


def downgrade():
    raise RuntimeError("Cannot downgrade")
    # ### commands auto generated by Alembic - please adjust! ###
    # ### end Alembic commands ###
