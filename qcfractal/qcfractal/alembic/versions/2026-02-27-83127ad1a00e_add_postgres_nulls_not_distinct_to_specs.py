"""Add postgres nulls not distinct to specs

Revision ID: 83127ad1a00e
Revises: 35bb042920a3
Create Date: 2026-02-27 20:40:36.102875

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "83127ad1a00e"
down_revision = "35bb042920a3"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("ux_neb_specification_keys", "neb_specification", type_="unique")
    op.drop_constraint("ux_reaction_specification_keys", "reaction_specification", type_="unique")

    op.create_unique_constraint(
        "ux_neb_specification_keys",
        "neb_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
        postgresql_nulls_not_distinct=True,
    )

    op.create_unique_constraint(
        "ux_reaction_specification_keys",
        "reaction_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
        postgresql_nulls_not_distinct=True,
    )

    pass


def downgrade():
    op.drop_constraint("ux_neb_specification_keys", "neb_specification", type_="unique")
    op.drop_constraint("ux_reaction_specification_keys", "reaction_specification", type_="unique")

    op.create_unique_constraint(
        "ux_neb_specification_keys",
        "neb_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
    )

    op.create_unique_constraint(
        "ux_reaction_specification_keys",
        "reaction_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
    )
