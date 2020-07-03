"""id(pirmary key) for torsion_init_mol

Revision ID: 1604623c481a
Revises: fb5bd88ae2f3
Create Date: 2020-07-02 18:42:17.267792

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1604623c481a'
down_revision = 'fb5bd88ae2f3'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("alter table torsion_init_mol_association add column id serial primary key;")


def downgrade():
    op.execute("alter table torsion_init_mol_association drop column id;")
