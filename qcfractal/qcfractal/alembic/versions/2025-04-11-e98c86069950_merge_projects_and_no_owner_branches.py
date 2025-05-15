"""merge projects and no_owner branches

Revision ID: e98c86069950
Revises: a036e75160ea, c5a3bed43646
Create Date: 2025-04-11 14:23:21.971882

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e98c86069950"
down_revision = ("a036e75160ea", "c5a3bed43646")
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("project_owner_group_id_fkey", "project", type_="foreignkey")
    op.drop_constraint("project_owner_user_id_owner_group_id_fkey", "project", type_="foreignkey")
    op.drop_index("ix_project_owner_group_id", table_name="project")
    op.drop_column("project", "owner_group_id")
    pass


def downgrade():
    raise NotImplementedError("Downgrade is not supported")
