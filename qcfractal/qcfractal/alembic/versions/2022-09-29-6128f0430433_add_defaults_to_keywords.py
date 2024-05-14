"""Add defaults to keywords

Revision ID: 6128f0430433
Revises: 77baa72171b9
Create Date: 2022-09-29 10:23:54.435874

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6128f0430433"
down_revision = "77baa72171b9"
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        sa.text(
            """
        UPDATE torsiondrive_specification
        SET keywords = (keywords || jsonb_build_object(
             'dihedrals', COALESCE(keywords->'dihedrals', '[]'::jsonb),
             'grid_spacing', COALESCE(keywords->'grid_spacing', '[]'::jsonb),
             'dihedral_ranges', COALESCE(keywords->'dihedral_ranges', Null::jsonb),
             'energy_decrease_thresh', COALESCE(keywords->'energy_decrease_thresh', Null::jsonb),
             'energy_upper_limit', COALESCE(keywords->'energy_upper_limit', Null::jsonb))
             )
        """
        )
    )

    op.execute(
        sa.text(
            """
        UPDATE gridoptimization_specification
        SET keywords = (keywords || jsonb_build_object(
             'scans', COALESCE(keywords->'scans', '[]'::jsonb),
             'preoptimization', COALESCE(keywords->'preoptimization', 'true'::jsonb))
             )
        """
        )
    )

    op.execute(
        sa.text(
            """
        UPDATE manybody_specification
        SET keywords = (keywords || jsonb_build_object('max_nbody', COALESCE(keywords->'max_nbody', Null::jsonb)))
        """
        )
    )


def downgrade():
    pass
