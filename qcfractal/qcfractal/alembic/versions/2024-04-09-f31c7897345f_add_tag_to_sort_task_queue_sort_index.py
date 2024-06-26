"""Add tag to sort task_queue sort index

Revision ID: f31c7897345f
Revises: 34d57d259c11
Create Date: 2024-04-09 09:50:13.771231

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f31c7897345f"
down_revision = "34d57d259c11"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_task_queue_sort", table_name="task_queue")
    op.create_index(
        "ix_task_queue_sort",
        "task_queue",
        [sa.text("priority DESC"), sa.text("sort_date ASC"), sa.text("id ASC"), "tag"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_task_queue_sort", table_name="task_queue")
    op.create_index("ix_task_queue_sort", "task_queue", [sa.text("priority DESC"), "sort_date", "id"], unique=False)
    # ### end Alembic commands ###
