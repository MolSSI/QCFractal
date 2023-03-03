"""clean up and migrate output store

Revision ID: 95e84c316d4d
Revises: 6f3de1040c37
Create Date: 2023-03-02 19:50:25.674718

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "95e84c316d4d"
down_revision = "6f3de1040c37"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("output_store", "data", new_column_name="old_data")
    op.alter_column("output_store", "compression", new_column_name="compression_type")
    op.add_column("output_store", sa.Column("data", sa.LargeBinary))
    op.execute(sa.text("ALTER TABLE output_store ALTER COLUMN data SET STORAGE EXTERNAL"))


def downgrade():
    raise RuntimeError("Cannot downgrade")
    pass
