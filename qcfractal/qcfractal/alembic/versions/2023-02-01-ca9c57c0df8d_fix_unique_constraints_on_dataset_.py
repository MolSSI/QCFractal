"""fix unique constraints on dataset record items

Revision ID: ca9c57c0df8d
Revises: 8c781020aed4
Create Date: 2023-02-01 10:19:01.065522

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "ca9c57c0df8d"
down_revision = "8c781020aed4"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    op.drop_constraint("ux_optimization_dataset_record_unique", "optimization_dataset_record")
    op.create_primary_key(
        "optimization_dataset_record_pkey",
        "optimization_dataset_record",
        ["dataset_id", "entry_name", "specification_name"],
    )

    op.drop_constraint("ux_singlepoint_dataset_record_unique", "singlepoint_dataset_record")
    op.create_primary_key(
        "singlepoint_dataset_record_pkey",
        "singlepoint_dataset_record",
        ["dataset_id", "entry_name", "specification_name"],
    )

    op.drop_constraint("ux_gridoptimization_dataset_record_unique", "gridoptimization_dataset_record")
    op.create_primary_key(
        "gridoptimization_dataset_record_pkey",
        "gridoptimization_dataset_record",
        ["dataset_id", "entry_name", "specification_name"],
    )

    op.drop_constraint("ux_torsiondrive_dataset_record_unique", "torsiondrive_dataset_record")
    op.create_primary_key(
        "torsiondrive_dataset_record_pkey",
        "torsiondrive_dataset_record",
        ["dataset_id", "entry_name", "specification_name"],
    )

    op.drop_constraint("ux_manybody_dataset_record_unique", "manybody_dataset_record")
    op.create_primary_key(
        "manybody_dataset_record_pkey", "manybody_dataset_record", ["dataset_id", "entry_name", "specification_name"]
    )

    op.drop_constraint("ux_reaction_dataset_record_unique", "reaction_dataset_record")
    op.create_primary_key(
        "reaction_dataset_record_pkey", "reaction_dataset_record", ["dataset_id", "entry_name", "specification_name"]
    )

    op.drop_constraint("ux_neb_dataset_record_unique", "neb_dataset_record")
    op.create_primary_key(
        "neb_dataset_record_pkey", "neb_dataset_record", ["dataset_id", "entry_name", "specification_name"]
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    raise RuntimeError("Cannot downgrade")
    # ### end Alembic commands ###