"""Update compute role for new endpoint

Revision ID: f8a7c273f18a
Revises: d03466436fec
Create Date: 2024-12-13 15:39:28.701912

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f8a7c273f18a"
down_revision = "d03466436fec"
branch_labels = None
depends_on = None


def upgrade():
    stmt = """
        UPDATE "role" SET permissions = jsonb_build_object(
                    'Statement', (permissions->'Statement')::jsonb || jsonb_build_object(
                               'Effect', 'Allow',
                               'Action', jsonb_build_array('READ'),
                               'Resource', '/compute/v1/information'
                )
            )::json
        WHERE rolename = 'compute'
        """

    op.execute(sa.text(stmt))


def downgrade():
    pass
