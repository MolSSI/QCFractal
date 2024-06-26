"""Add find_existing to services

Revision ID: e6f5053c7600
Revises: 1abf80db6c19
Create Date: 2023-08-13 17:01:08.736187

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e6f5053c7600"
down_revision = "1abf80db6c19"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("service_queue", sa.Column("find_existing", sa.Boolean(), nullable=True))
    op.execute(sa.text("UPDATE service_queue SET find_existing = true;"))
    op.alter_column("service_queue", "find_existing", nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("service_queue", "find_existing")
    # ### end Alembic commands ###
