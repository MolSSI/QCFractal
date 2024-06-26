"""enable different optimizations in neb

Revision ID: 1638db43303c
Revises: f9d784e816d2
Create Date: 2023-06-05 11:25:35.953639

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1638db43303c"
down_revision = "f9d784e816d2"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("neb_specification", sa.Column("optimization_specification_id", sa.Integer(), nullable=True))
    op.drop_constraint("ux_neb_specification_keys", "neb_specification", type_="unique")
    op.create_unique_constraint(
        "ux_neb_specification_keys",
        "neb_specification",
        ["program", "singlepoint_specification_id", "optimization_specification_id", "keywords_hash"],
    )
    op.create_index(
        "ix_neb_specification_optimization_specification_id",
        "neb_specification",
        ["optimization_specification_id"],
        unique=False,
    )
    op.create_foreign_key(
        None, "neb_specification", "optimization_specification", ["optimization_specification_id"], ["id"]
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, "neb_specification", type_="foreignkey")
    op.drop_index("ix_neb_specification_optimization_specification_id", table_name="neb_specification")
    op.drop_constraint("ux_neb_specification_keys", "neb_specification", type_="unique")
    op.create_unique_constraint(
        "ux_neb_specification_keys", "neb_specification", ["program", "singlepoint_specification_id", "keywords_hash"]
    )
    op.drop_column("neb_specification", "optimization_specification_id")
    # ### end Alembic commands ###
