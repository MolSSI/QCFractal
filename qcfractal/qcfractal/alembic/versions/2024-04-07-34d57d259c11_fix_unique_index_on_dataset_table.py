"""Fix unique index on dataset table

Revision ID: 34d57d259c11
Revises: 6b24c66979ab
Create Date: 2024-04-07 09:36:05.692066

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "34d57d259c11"
down_revision = "6b24c66979ab"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.execute("ALTER TABLE base_dataset DROP CONSTRAINT IF EXISTS ux_base_dataset_dataset_type_lname")
    op.create_unique_constraint("ux_base_dataset_dataset_type_lname", "base_dataset", ["dataset_type", "lname"])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    pass
    # ### end Alembic commands ###
