"""gridoptimization optimization key to str

Revision ID: e8bba86921a5
Revises: f3ad208b70da
Create Date: 2022-04-07 16:49:34.483749

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e8bba86921a5"
down_revision = "f3ad208b70da"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE gridoptimization_optimization SET key = 'preoptimization' WHERE key = '\"preoptimization\"'")


def downgrade():
    raise RuntimeError("Cannot downgrade")
