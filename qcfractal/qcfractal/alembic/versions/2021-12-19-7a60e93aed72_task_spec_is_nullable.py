"""task spec is nullable

Revision ID: 7a60e93aed72
Revises: bf31e6366a10
Create Date: 2021-12-19 11:39:43.489411

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "7a60e93aed72"
down_revision = "bf31e6366a10"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("task_queue", "spec", existing_type=postgresql.BYTEA(), nullable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("task_queue", "spec", existing_type=postgresql.BYTEA(), nullable=False)
    # ### end Alembic commands ###