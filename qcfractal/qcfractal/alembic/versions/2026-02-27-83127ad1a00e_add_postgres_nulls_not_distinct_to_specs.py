"""Add postgres nulls not distinct to specs

Revision ID: 83127ad1a00e
Revises: 35bb042920a3
Create Date: 2026-02-27 20:40:36.102875

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "83127ad1a00e"
down_revision = "35bb042920a3"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint("ux_neb_specification_keys", "neb_specification", type_="unique")
    op.drop_constraint("ux_reaction_specification_keys", "reaction_specification", type_="unique")

    # We need to find duplicates due to a NULL optimization_specification_id
    # then update references and delete
    op.execute(sa.text("""
        WITH ranked AS (
            SELECT
                id,
                MIN(id) OVER (
                    PARTITION BY specification_hash, singlepoint_specification_id
                    ) AS canonical_id
            FROM neb_specification
            WHERE optimization_specification_id IS NULL
        ),
             dupes AS (
                 SELECT id AS duplicate_id, canonical_id
                 FROM ranked
                 WHERE id <> canonical_id
             )
        UPDATE neb_record r
        SET specification_id = d.canonical_id
        FROM dupes d
        WHERE r.specification_id = d.duplicate_id;
    """))

    op.execute(sa.text("""
        WITH ranked AS (
            SELECT
                id,
                MIN(id) OVER (
                    PARTITION BY specification_hash, singlepoint_specification_id
                    ) AS canonical_id
            FROM neb_specification
            WHERE optimization_specification_id IS NULL
        ),
             dupes AS (
                 SELECT id AS duplicate_id, canonical_id
                 FROM ranked
                 WHERE id <> canonical_id
             )
        UPDATE neb_dataset_specification rds
        SET specification_id = d.canonical_id
        FROM dupes d
        WHERE rds.specification_id = d.duplicate_id;
    """))

    op.execute(sa.text("""
        WITH ranked AS (
            SELECT
                id,
                MIN(id) OVER (
                    PARTITION BY specification_hash, singlepoint_specification_id
                    ) AS canonical_id
            FROM neb_specification
            WHERE optimization_specification_id IS NULL
        )
        DELETE FROM neb_specification r
            USING ranked d
        WHERE r.id = d.id
          AND d.id <> d.canonical_id;
    """))

    # Now for reactions, which can have singlepoint or optimization specification ids be null
    op.execute(sa.text("""
        WITH ranked AS (
            SELECT
                id,
                MIN(id) OVER (
                    PARTITION BY specification_hash, singlepoint_specification_id, optimization_specification_id
                    ) AS canonical_id
            FROM reaction_specification
        ),
             dupes AS (
                 SELECT id AS duplicate_id, canonical_id
                 FROM ranked
                 WHERE id <> canonical_id
             )
        UPDATE reaction_record r
        SET specification_id = d.canonical_id
        FROM dupes d
        WHERE r.specification_id = d.duplicate_id;
    """))

    op.execute(sa.text("""
        WITH ranked AS (
            SELECT
                id,
                MIN(id) OVER (
                    PARTITION BY specification_hash, singlepoint_specification_id, optimization_specification_id
                    ) AS canonical_id
            FROM reaction_specification
        ),
             dupes AS (
                 SELECT id AS duplicate_id, canonical_id
                 FROM ranked
                 WHERE id <> canonical_id
             )
        UPDATE reaction_dataset_specification rds
        SET specification_id = d.canonical_id
        FROM dupes d
        WHERE rds.specification_id = d.duplicate_id;
    """))

    op.execute(sa.text("""
        WITH ranked AS (
            SELECT
                id,
                MIN(id) OVER (
                    PARTITION BY specification_hash, singlepoint_specification_id, optimization_specification_id
                    ) AS canonical_id
            FROM reaction_specification
        )
        DELETE FROM reaction_specification r
            USING ranked d
        WHERE r.id = d.id
          AND d.id <> d.canonical_id;
    """))

    op.create_unique_constraint(
        "ux_neb_specification_keys",
        "neb_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
        postgresql_nulls_not_distinct=True,
    )

    op.create_unique_constraint(
        "ux_reaction_specification_keys",
        "reaction_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
        postgresql_nulls_not_distinct=True,
    )

    pass


def downgrade():
    op.drop_constraint("ux_neb_specification_keys", "neb_specification", type_="unique")
    op.drop_constraint("ux_reaction_specification_keys", "reaction_specification", type_="unique")

    op.create_unique_constraint(
        "ux_neb_specification_keys",
        "neb_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
    )

    op.create_unique_constraint(
        "ux_reaction_specification_keys",
        "reaction_specification",
        ["specification_hash", "singlepoint_specification_id", "optimization_specification_id"],
    )
