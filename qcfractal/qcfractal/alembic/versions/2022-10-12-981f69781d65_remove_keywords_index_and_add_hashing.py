"""Remove keywords index and add hashing

Revision ID: 981f69781d65
Revises: 148fef89c2ec
Create Date: 2022-10-12 15:34:38.809659

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "981f69781d65"
down_revision = "148fef89c2ec"
branch_labels = None
depends_on = None


from migration_helpers.hashing import hash_dict_1


def create_hashes(table):
    conn = op.get_bind()
    res = conn.execute(sa.text(f"SELECT id,keywords FROM {table};"))
    all_kw = res.fetchall()

    for spec_id, kw in all_kw:
        h = hash_dict_1(kw)
        op.execute(sa.text(f"""UPDATE {table} SET keywords_hash = '{h}' WHERE id = {spec_id};"""))


def upgrade():
    op.execute(sa.text("DROP INDEX IF EXISTS ix_qc_specification_keywords"))
    op.execute(sa.text("ALTER TABLE qc_specification DROP CONSTRAINT IF EXISTS ux_qc_specification_keys"))
    op.add_column("qc_specification", sa.Column("keywords_hash", sa.String()))
    op.drop_index("ix_qc_specification_protocols")
    op.create_unique_constraint(
        "ux_qc_specification_keys",
        "qc_specification",
        ["program", "driver", "method", "basis", "keywords_hash", "protocols"],
    )
    create_hashes("qc_specification")
    op.alter_column("qc_specification", "keywords_hash", nullable=False)

    op.execute(sa.text("DROP INDEX IF EXISTS ix_optimization_specification_keywords"))
    op.execute(
        sa.text("ALTER TABLE optimization_specification DROP CONSTRAINT IF EXISTS ux_optimization_specification_keys")
    )
    op.drop_index("ix_optimization_specification_protocols")
    op.add_column("optimization_specification", sa.Column("keywords_hash", sa.String()))
    op.create_unique_constraint(
        "ux_optimization_specification_keys",
        "optimization_specification",
        ["program", "qc_specification_id", "keywords_hash", "protocols"],
    )
    create_hashes("optimization_specification")
    op.alter_column("optimization_specification", "keywords_hash", nullable=False)

    op.execute(sa.text("DROP INDEX IF EXISTS ix_torsiondrive_specification_keywords"))
    op.execute(
        sa.text("ALTER TABLE torsiondrive_specification DROP CONSTRAINT IF EXISTS ux_torsiondrive_specification_keys")
    )
    op.add_column("torsiondrive_specification", sa.Column("keywords_hash", sa.String()))
    op.create_unique_constraint(
        "ux_torsiondrive_specification_keys",
        "torsiondrive_specification",
        ["program", "optimization_specification_id", "keywords_hash"],
    )
    create_hashes("torsiondrive_specification")
    op.alter_column("torsiondrive_specification", "keywords_hash", nullable=False)

    op.execute(sa.text("DROP INDEX IF EXISTS ix_gridoptimization_specification_keywords"))
    op.execute(
        sa.text(
            "ALTER TABLE gridoptimization_specification DROP CONSTRAINT IF EXISTS ux_gridoptimization_specification_keys"
        )
    )
    op.add_column("gridoptimization_specification", sa.Column("keywords_hash", sa.String()))
    op.create_unique_constraint(
        "ux_gridoptimization_specification_keys",
        "gridoptimization_specification",
        ["program", "optimization_specification_id", "keywords_hash"],
    )
    create_hashes("gridoptimization_specification")
    op.alter_column("gridoptimization_specification", "keywords_hash", nullable=False)

    op.execute(sa.text("DROP INDEX IF EXISTS ix_manybody_specification_keywords"))
    op.execute(sa.text("ALTER TABLE manybody_specification DROP CONSTRAINT IF EXISTS ux_manybody_specification_keys"))
    op.add_column("manybody_specification", sa.Column("keywords_hash", sa.String()))
    op.create_unique_constraint(
        "ux_manybody_specification_keys",
        "manybody_specification",
        ["program", "singlepoint_specification_id", "keywords_hash"],
    )
    create_hashes("manybody_specification")
    op.alter_column("manybody_specification", "keywords_hash", nullable=False)

    op.execute(sa.text("DROP INDEX IF EXISTS ix_reaction_specification_keywords"))
    op.execute(sa.text("ALTER TABLE reaction_specification DROP CONSTRAINT IF EXISTS ux_reaction_specification_keys"))
    op.add_column("reaction_specification", sa.Column("keywords_hash", sa.String()))
    op.create_unique_constraint(
        "ux_reaction_specification_keys",
        "reaction_specification",
        ["program", "singlepoint_specification_id", "optimization_specification_id", "keywords_hash"],
    )
    create_hashes("reaction_specification")
    op.alter_column("reaction_specification", "keywords_hash", nullable=False)

    op.execute(sa.text("DROP INDEX IF EXISTS ix_neb_specification_keywords"))
    op.execute(sa.text("ALTER TABLE neb_specification DROP CONSTRAINT IF EXISTS ux_neb_specification_keys"))
    op.add_column("neb_specification", sa.Column("keywords_hash", sa.String()))
    op.create_unique_constraint(
        "ux_neb_specification_keys", "neb_specification", ["program", "singlepoint_specification_id", "keywords_hash"]
    )
    create_hashes("neb_specification")
    op.alter_column("neb_specification", "keywords_hash", nullable=False)


def downgrade():
    raise RuntimeError("CANNOT DOWNGRADE")
