"""Add dataset attachment table

Revision ID: 84285e3620fd
Revises: 02afa97249c7
Create Date: 2025-01-03 10:04:16.201770

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "84285e3620fd"
down_revision = "02afa97249c7"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "dataset_attachment",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("dataset_id", sa.Integer(), nullable=False),
        sa.Column("attachment_type", sa.Enum("other", "view", name="datasetattachmenttype"), nullable=False),
        sa.ForeignKeyConstraint(["dataset_id"], ["base_dataset.id"], ondelete="cascade"),
        sa.ForeignKeyConstraint(["id"], ["external_file.id"], ondelete="cascade"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_dataset_attachment_dataset_id", "dataset_attachment", ["dataset_id"], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_dataset_attachment_dataset_id", table_name="dataset_attachment")
    op.drop_table("dataset_attachment")
    # ### end Alembic commands ###
