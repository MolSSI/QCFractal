"""Remove tasks for completed calculations

Revision ID: 038ffd952a00
Revises: 9a6345275dba
Create Date: 2021-09-11 11:31:35.383767

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "038ffd952a00"
down_revision = "9a6345275dba"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "DELETE FROM task_queue tq USING base_result br WHERE tq.base_result_id = br.id AND br.status = 'complete'"
        )
    )


def downgrade():
    pass
