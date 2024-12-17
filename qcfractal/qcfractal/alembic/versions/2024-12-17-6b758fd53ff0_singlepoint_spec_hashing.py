"""Singlepoint spec hashing

Revision ID: 6b758fd53ff0
Revises: 8263992eb6c8
Create Date: 2024-12-17 10:05:11.756731

"""

import sqlalchemy as sa
from alembic import op

from migration_helpers.hashing import hash_dict_1

# revision identifiers, used by Alembic.
revision = "6b758fd53ff0"
down_revision = "8263992eb6c8"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("qc_specification", sa.Column("specification_hash", sa.String(), nullable=True))

    # Generate hashes for existing specs
    conn = op.get_bind()
    res = conn.execute(
        sa.text(f"SELECT id, program, driver, method, basis, keywords, protocols FROM qc_specification;")
    )
    all_spec = res.fetchall()

    for spec_id, program, driver, method, basis, keywords, protocols in all_spec:
        d = {
            "program": program,
            "driver": driver,
            "method": method,
            "basis": basis,
            "keywords": keywords,
            "protocols": protocols,
        }
        h = hash_dict_1(d)
        op.execute(sa.text(f"""UPDATE qc_specification SET specification_hash = '{h}' WHERE id = {spec_id};"""))

    op.alter_column("qc_specification", "specification_hash", nullable=False)
    op.drop_constraint("ux_qc_specification_keys", "qc_specification", type_="unique")
    op.drop_column("qc_specification", "keywords_hash")
    op.create_unique_constraint("ux_qc_specification_specification_hash", "qc_specification", ["specification_hash"])
    # ### end Alembic commands ###


def downgrade():
    raise RuntimeError("Downgrade not supported.")
