"""add owner_user to base_record/base_dataset, creator_user to project

Revision ID: 384d9ebfef73
Revises: c9d30a5a6c9e
Create Date: 2026-09-28 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "384d9ebfef73"
down_revision = "c9d30a5a6c9e"
branch_labels = None
depends_on = None


def upgrade():
    # Records and datasets have creator_user_id (renamed from owner_user_id in c5a3bed43646),
    # projects have owner_user_id. Add the missing one of the pair to each, copied from the
    # existing column - the owner and the creator are currently always the same user.
    op.add_column("base_record", sa.Column("owner_user_id", sa.Integer(), nullable=True))
    op.create_foreign_key("base_record_owner_user_id_fkey", "base_record", "user", ["owner_user_id"], ["id"])
    op.create_index("ix_base_record_owner_user_id", "base_record", ["owner_user_id"], unique=False)
    op.execute(sa.text("UPDATE base_record SET owner_user_id = creator_user_id"))

    op.add_column("base_dataset", sa.Column("owner_user_id", sa.Integer(), nullable=True))
    op.create_foreign_key("base_dataset_owner_user_id_fkey", "base_dataset", "user", ["owner_user_id"], ["id"])
    op.create_index("ix_base_dataset_owner_user_id", "base_dataset", ["owner_user_id"], unique=False)
    op.execute(sa.text("UPDATE base_dataset SET owner_user_id = creator_user_id"))

    op.add_column("project", sa.Column("creator_user_id", sa.Integer(), nullable=True))
    op.create_foreign_key("project_creator_user_id_fkey", "project", "user", ["creator_user_id"], ["id"])
    op.create_index("ix_project_creator_user_id", "project", ["creator_user_id"], unique=False)
    op.execute(sa.text("UPDATE project SET creator_user_id = owner_user_id"))


def downgrade():
    op.drop_index("ix_project_creator_user_id", table_name="project")
    op.drop_constraint("project_creator_user_id_fkey", "project", type_="foreignkey")
    op.drop_column("project", "creator_user_id")

    op.drop_index("ix_base_dataset_owner_user_id", table_name="base_dataset")
    op.drop_constraint("base_dataset_owner_user_id_fkey", "base_dataset", type_="foreignkey")
    op.drop_column("base_dataset", "owner_user_id")

    op.drop_index("ix_base_record_owner_user_id", table_name="base_record")
    op.drop_constraint("base_record_owner_user_id_fkey", "base_record", type_="foreignkey")
    op.drop_column("base_record", "owner_user_id")
