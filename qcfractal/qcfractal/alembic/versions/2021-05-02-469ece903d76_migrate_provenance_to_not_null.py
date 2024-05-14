"""migrate provenance to not null

Revision ID: 469ece903d76
Revises: 6b07e9a3589d
Create Date: 2021-05-02 09:48:57.061825

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.orm.session import Session


# revision identifiers, used by Alembic.
revision = "469ece903d76"
down_revision = "6b07e9a3589d"
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "UPDATE base_result SET provenance = provenance::jsonb || '{\"creator\":\"\"}' where (provenance->'creator')::text = 'null'"
        )
    )
    conn.execute(
        sa.text(
            "UPDATE base_result SET provenance = provenance::jsonb || '{\"routine\":\"\"}' where (provenance->'routine')::text = 'null'"
        )
    )
    conn.execute(
        sa.text(
            "UPDATE base_result SET provenance = provenance::jsonb || '{\"version\":\"\"}' where (provenance->'version')::text = 'null'"
        )
    )
    conn.execute(
        sa.text(
            "UPDATE molecule SET provenance = provenance::jsonb || '{\"creator\":\"\"}' where (provenance->'creator')::text = 'null'"
        )
    )
    conn.execute(
        sa.text(
            "UPDATE molecule SET provenance = provenance::jsonb || '{\"routine\":\"\"}' where (provenance->'routine')::text = 'null'"
        )
    )
    conn.execute(
        sa.text(
            "UPDATE molecule SET provenance = provenance::jsonb || '{\"version\":\"\"}' where (provenance->'version')::text = 'null'"
        )
    )
    conn.execute(sa.text("UPDATE molecule SET connectivity = null where connectivity::text = '[]'"))
    conn.execute(
        sa.text(
            "UPDATE result SET properties = properties::jsonb - 'mp2_total_correlation_energy' || jsonb_build_object('mp2_correlation_energy', properties->'mp2_total_correlation_energy') WHERE properties::jsonb ? 'mp2_total_correlation_energy'"
        )
    )


def downgrade():
    pass
