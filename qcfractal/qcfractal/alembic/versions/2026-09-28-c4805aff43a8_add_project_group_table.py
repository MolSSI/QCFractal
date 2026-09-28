"""add project_group table

Revision ID: c4805aff43a8
Revises: 4b91b20ffb2d
Create Date: 2026-09-28 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c4805aff43a8"
down_revision = "384d9ebfef73"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "project_group",
        sa.Column("project_id", sa.Integer(), nullable=False),
        sa.Column("group_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["group.id"]),
        sa.ForeignKeyConstraint(["project_id"], ["project.id"], ondelete="cascade"),
        sa.PrimaryKeyConstraint("project_id", "group_id"),
    )


def downgrade():
    op.drop_table("project_group")
