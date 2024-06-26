"""Add indices to access and base_result

Revision ID: cd3d3e2915ce
Revises: c05d63683601
Create Date: 2021-03-08 09:40:56.987985

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "cd3d3e2915ce"
down_revision = "c05d63683601"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("access_type", table_name="access_log")
    op.create_index(op.f("ix_access_log_access_date"), "access_log", ["access_date"], unique=False)
    op.create_index(op.f("ix_access_log_access_type"), "access_log", ["access_type"], unique=False)
    op.create_index("ix_base_result_hash_index", "base_result", ["hash_index"], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_base_result_hash_index", table_name="base_result")
    op.drop_index(op.f("ix_access_log_access_type"), table_name="access_log")
    op.drop_index(op.f("ix_access_log_access_date"), table_name="access_log")
    op.create_index("access_type", "access_log", ["access_date"], unique=False)
    # ### end Alembic commands ###
