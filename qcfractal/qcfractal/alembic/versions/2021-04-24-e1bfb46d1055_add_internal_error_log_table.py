"""Add internal_error_log table

Revision ID: e1bfb46d1055
Revises: 9e817a4a5cec
Create Date: 2021-04-24 13:57:02.485403

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e1bfb46d1055"
down_revision = "9e817a4a5cec"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "internal_error_log",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("error_date", sa.DateTime(), nullable=True),
        sa.Column("qcfractal_version", sa.String(), nullable=True),
        sa.Column("error_text", sa.String(), nullable=True),
        sa.Column("user", sa.String(), nullable=True),
        sa.Column("request_path", sa.String(), nullable=True),
        sa.Column("request_headers", sa.String(), nullable=True),
        sa.Column("request_body", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_internal_error_log_error_date"), "internal_error_log", ["error_date"], unique=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_internal_error_log_error_date"), table_name="internal_error_log")
    op.drop_table("internal_error_log")
    # ### end Alembic commands ###
