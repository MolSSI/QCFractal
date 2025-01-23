"""delete old manybody

Revision ID: a5a701dc344d
Revises: 73b4838a6839
Create Date: 2024-06-11 15:51:11.380308

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a5a701dc344d"
down_revision = "73b4838a6839"
branch_labels = None
depends_on = None


def upgrade():

    conn = op.get_bind()

    res = conn.execute(sa.text("SELECT count(*) FROM base_record WHERE record_type = 'manybody'"))
    count = res.fetchone()[0]
    if count != 0:
        raise ValueError("Will not delete old manybody tables with existing data")

    res = conn.execute(sa.text("SELECT count(*) FROM manybody_record"))
    count = res.fetchone()[0]
    if count != 0:
        raise ValueError("Will not delete old manybody tables with existing data")

    res = conn.execute(sa.text("SELECT count(*) FROM manybody_dataset"))
    count = res.fetchone()[0]
    if count != 0:
        raise ValueError("Will not delete old manybody tables with existing data")

    op.execute(sa.text("DROP TABLE manybody_cluster CASCADE"))
    op.execute(sa.text("DROP TABLE manybody_dataset CASCADE"))
    op.execute(sa.text("DROP TABLE manybody_dataset_entry CASCADE"))
    op.execute(sa.text("DROP TABLE manybody_dataset_record CASCADE"))
    op.execute(sa.text("DROP TABLE manybody_dataset_specification CASCADE"))
    op.execute(sa.text("DROP TABLE manybody_record CASCADE"))
    op.execute(sa.text("DROP TABLE manybody_specification CASCADE"))


def downgrade():
    raise NotImplementedError("Downgrade not supported")
