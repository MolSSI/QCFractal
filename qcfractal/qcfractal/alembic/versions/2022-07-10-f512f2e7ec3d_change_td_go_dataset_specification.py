"""change TD/GO dataset specification

Revision ID: f512f2e7ec3d
Revises: 5db09491c69d
Create Date: 2022-07-10 09:45:24.956165

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f512f2e7ec3d"
down_revision = "5db09491c69d"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    #######################
    # Gridoptimization
    #######################
    op.add_column(
        "gridoptimization_dataset_specification", sa.Column("new_specification_id", sa.Integer(), nullable=True)
    )
    op.create_foreign_key(
        None,
        "gridoptimization_dataset_specification",
        "gridoptimization_specification",
        ["new_specification_id"],
        ["id"],
    )

    op.execute(
        sa.text(
            """
        INSERT INTO gridoptimization_specification (program, optimization_specification_id, keywords)
        SELECT 'gridoptimization', specification_id, '{}'::jsonb
        FROM gridoptimization_dataset_specification
        ON CONFLICT DO NOTHING"""
        )
    )

    op.execute(
        sa.text(
            """
        UPDATE gridoptimization_dataset_specification
        SET new_specification_id = (
            SELECT id FROM gridoptimization_specification
            WHERE optimization_specification_id = specification_id
            AND keywords = '{}'::jsonb
        )"""
        )
    )

    op.drop_column("gridoptimization_dataset_specification", "specification_id")
    op.alter_column(
        "gridoptimization_dataset_specification",
        "new_specification_id",
        nullable=False,
        new_column_name="specification_id",
    )
    op.alter_column(
        "gridoptimization_dataset_entry", "additional_keywords", new_column_name="additional_optimization_keywords"
    )
    op.alter_column(
        "gridoptimization_dataset_entry", "gridoptimization_keywords", new_column_name="additional_keywords"
    )

    op.drop_constraint(
        "gridoptimization_dataset_specificatio_new_specification_id_fkey", "gridoptimization_dataset_specification"
    )
    op.create_index(
        "ix_gridoptimization_dataset_specification_specification_id",
        "gridoptimization_dataset_specification",
        ["specification_id"],
    )
    op.create_foreign_key(
        "gridoptimization_dataset_specification_specification_id_fkey",
        "gridoptimization_dataset_specification",
        "gridoptimization_specification",
        ["specification_id"],
        ["id"],
    )

    #######################
    # Torsiondrive
    #######################
    op.add_column("torsiondrive_dataset_specification", sa.Column("new_specification_id", sa.Integer(), nullable=True))
    op.create_foreign_key(
        None, "torsiondrive_dataset_specification", "torsiondrive_specification", ["new_specification_id"], ["id"]
    )

    op.execute(
        sa.text(
            """
        INSERT INTO torsiondrive_specification (program, optimization_specification_id, keywords)
        SELECT 'torsiondrive', specification_id, '{}'::jsonb
        FROM torsiondrive_dataset_specification
        ON CONFLICT DO NOTHING"""
        )
    )

    op.execute(
        sa.text(
            """
        UPDATE torsiondrive_dataset_specification
        SET new_specification_id = (
            SELECT id FROM torsiondrive_specification
            WHERE optimization_specification_id = specification_id
            AND keywords = '{}'::jsonb
        )"""
        )
    )

    op.drop_column("torsiondrive_dataset_specification", "specification_id")
    op.alter_column(
        "torsiondrive_dataset_specification", "new_specification_id", nullable=False, new_column_name="specification_id"
    )
    op.alter_column(
        "torsiondrive_dataset_entry", "additional_keywords", new_column_name="additional_optimization_keywords"
    )
    op.alter_column("torsiondrive_dataset_entry", "torsiondrive_keywords", new_column_name="additional_keywords")

    op.drop_constraint(
        "torsiondrive_dataset_specification_new_specification_id_fkey", "torsiondrive_dataset_specification"
    )
    op.create_foreign_key(
        "torsiondrive_dataset_specification_specification_id_fkey",
        "torsiondrive_dataset_specification",
        "torsiondrive_specification",
        ["specification_id"],
        ["id"],
    )
    op.create_index(
        "ix_torsiondrive_dataset_specification_specification_id",
        "torsiondrive_dataset_specification",
        ["specification_id"],
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    raise RuntimeError("Cannot downgrade")
    # ### end Alembic commands ###
