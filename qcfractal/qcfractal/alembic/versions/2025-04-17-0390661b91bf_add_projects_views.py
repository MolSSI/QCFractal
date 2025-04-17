"""Add projects views

Revision ID: 0390661b91bf
Revises: 56ef8ac1765b
Create Date: 2025-04-17 10:26:09.369279

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0390661b91bf"
down_revision = "56ef8ac1765b"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        sa.text(
            """
                create or replace view project_records_view as  SELECT project_record.project_id,
                    project_record.record_id
                   FROM project_record
                UNION
                 SELECT project_record.project_id,
                    record_children_view.child_id AS record_id
                   FROM (project_record
                     JOIN record_children_view ON ((project_record.record_id = record_children_view.parent_id)))
                UNION
                 SELECT project_dataset.project_id,
                    dataset_direct_records_view.record_id
                   FROM (project_dataset
                     JOIN dataset_direct_records_view ON ((project_dataset.dataset_id = dataset_direct_records_view.dataset_id))); 
    """
        )
    )


def downgrade():
    op.execute(sa.text("DROP VIEW IF EXISTS project_records_view"))
    pass
