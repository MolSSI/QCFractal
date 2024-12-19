"""NEB spec hashing

Revision ID: 0587bb0220aa
Revises: a3c51b03bc19
Create Date: 2024-12-18 10:18:17.162740

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from migration_helpers.hashing import hash_dict_1

# revision identifiers, used by Alembic.
revision = "0587bb0220aa"
down_revision = "a3c51b03bc19"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("neb_specification", sa.Column("specification_hash", sa.String(), nullable=True))
    op.add_column("neb_specification", sa.Column("protocols", postgresql.JSONB(astext_type=sa.Text()), nullable=True))

    # Generate hashes for existing specs
    conn = op.get_bind()
    res = conn.execute(sa.text(f"SELECT id, program, keywords FROM neb_specification;"))
    all_spec = res.fetchall()
    for spec_id, program, keywords in all_spec:
        d = {
            "program": program,
            "keywords": keywords,
            "protocols": {},  # Default for now
        }
        h = hash_dict_1(d)
        op.execute(sa.text(f"""UPDATE neb_specification SET specification_hash = '{h}' WHERE id = {spec_id};"""))

    op.execute(sa.text("UPDATE neb_specification SET protocols = '{}'::JSONB"))

    op.alter_column("neb_specification", "protocols", nullable=False)
    op.alter_column("neb_specification", "specification_hash", nullable=False)
    op.drop_constraint("ux_neb_specification_keys", "neb_specification", type_="unique")
    op.create_unique_constraint(
        "ux_neb_specification_keys",
        "neb_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
    )
    op.drop_column("neb_specification", "keywords_hash")

    op.create_check_constraint(
        "ck_neb_specification_program_lower", "neb_specification", sa.text("program = LOWER(program)")
    )

    # ### end Alembic commands ###


def downgrade():
    raise RuntimeError("Downgrade not supported.")