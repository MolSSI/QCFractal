"""clean up native files

Revision ID: 1a61b3bb1ee4
Revises: 1c46a35bf565
Create Date: 2023-02-08 12:22:48.401847

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1a61b3bb1ee4"
down_revision = "1c46a35bf565"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("native_file", "is_text")
    op.drop_column("native_file", "uncompressed_size")

    op.alter_column("native_file", "compression", new_column_name="compression_type")

    op.execute(sa.text("ALTER TABLE native_file ALTER COLUMN data SET STORAGE EXTERNAL"))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    raise RuntimeError("Cannot downgrade")
