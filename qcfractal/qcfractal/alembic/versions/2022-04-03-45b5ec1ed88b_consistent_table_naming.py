"""Consistent table naming

Revision ID: 45b5ec1ed88b
Revises: 6565544dfb94
Create Date: 2022-04-03 08:59:48.992053

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "45b5ec1ed88b"
down_revision = "6565544dfb94"
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table("record_comments", "record_comment")
    op.execute(sa.text("ALTER SEQUENCE record_comments_id_seq RENAME TO record_comment_id_seq"))
    op.execute(sa.text("ALTER INDEX record_comments_pkey RENAME TO record_comment_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE record_comment RENAME CONSTRAINT record_comments_record_id_fkey TO record_comment_record_id_fkey"
        )
    )

    op.rename_table("compute_manager_logs", "compute_manager_log")
    op.execute(sa.text("ALTER SEQUENCE compute_manager_logs_id_seq RENAME TO compute_manager_log_id_seq"))
    op.execute(sa.text("ALTER INDEX ix_compute_manager_logs_timestamp RENAME TO ix_compute_manager_log_timestamp"))
    op.execute(sa.text("ALTER INDEX compute_manager_logs_pkey RENAME TO compute_manager_log_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE compute_manager_log RENAME CONSTRAINT compute_manager_logs_manager_id_fkey TO compute_manager_log_manager_id_fkey"
        )
    )

    op.rename_table("reaction_components", "reaction_component")
    op.execute(sa.text("ALTER INDEX reaction_components_pkey RENAME TO reaction_component_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE reaction_component RENAME CONSTRAINT reaction_components_molecule_id_fkey TO reaction_component_molecule_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE reaction_component RENAME CONSTRAINT reaction_components_singlepoint_id_fkey TO reaction_component_singlepoint_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE reaction_component RENAME CONSTRAINT reaction_components_reaction_id_fkey TO reaction_component_reaction_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE reaction_component RENAME CONSTRAINT reaction_components_reaction_id_molecule_id_fkey TO reaction_component_reaction_id_molecule_id_fkey"
        )
    )

    op.rename_table("reaction_stoichiometries", "reaction_stoichiometry")
    op.execute(sa.text("ALTER INDEX reaction_stoichiometries_pkey RENAME TO reaction_stoichiometry_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE reaction_stoichiometry RENAME CONSTRAINT reaction_stoichiometries_molecule_id_fkey TO reaction_stoichiometry_molecule_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE reaction_stoichiometry RENAME CONSTRAINT reaction_stoichiometries_reaction_id_fkey TO reaction_stoichiometry_reaction_id_fkey"
        )
    )

    op.rename_table("service_dependencies", "service_dependency")
    op.execute(sa.text("ALTER SEQUENCE service_dependencies_id_seq RENAME TO service_dependency_id_seq"))
    op.execute(sa.text("ALTER INDEX service_dependencies_pkey RENAME TO service_dependency_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE service_dependency RENAME CONSTRAINT service_dependencies_record_id_fkey TO service_dependency_record_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE service_dependency RENAME CONSTRAINT service_dependencies_service_id_fkey TO service_dependency_service_id_fkey"
        )
    )
    op.execute(
        sa.text("ALTER TABLE service_dependency RENAME CONSTRAINT ux_service_dependencies TO ux_service_dependency")
    )

    op.rename_table("gridoptimization_optimizations", "gridoptimization_optimization")
    op.execute(sa.text("ALTER INDEX gridoptimization_optimizations_pkey RENAME TO gridoptimization_optimization_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE gridoptimization_optimization RENAME CONSTRAINT gridoptimization_optimizations_gridoptimization_id_fkey TO gridoptimization_optimization_gridoptimization_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE gridoptimization_optimization RENAME CONSTRAINT gridoptimization_optimizations_optimization_id_fkey TO gridoptimization_optimization_optimization_id_fkey"
        )
    )

    op.rename_table("torsiondrive_optimizations", "torsiondrive_optimization")
    op.execute(sa.text("ALTER INDEX torsiondrive_optimizations_pkey RENAME TO torsiondrive_optimization_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE torsiondrive_optimization RENAME CONSTRAINT torsiondrive_optimizations_optimization_id_fkey TO torsiondrive_optimization_optimization_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE torsiondrive_optimization RENAME CONSTRAINT torsiondrive_optimizations_torsiondrive_id_fkey TO torsiondrive_optimization_torsiondrive_id_fkey"
        )
    )

    op.rename_table("torsiondrive_initial_molecules", "torsiondrive_initial_molecule")
    op.execute(sa.text("ALTER INDEX torsiondrive_initial_molecules_pkey RENAME TO torsiondrive_initial_molecule_pkey"))
    op.execute(
        sa.text(
            "ALTER TABLE torsiondrive_initial_molecule RENAME CONSTRAINT torsiondrive_initial_molecules_molecule_id_fkey TO torsiondrive_initial_molecule_molecule_id_fkey"
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE torsiondrive_initial_molecule RENAME CONSTRAINT torsiondrive_initial_molecules_torsiondrive_id_fkey TO torsiondrive_initial_molecule_torsiondrive_id_fkey"
        )
    )

    # Now renaming stuff in the server logs
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'compute_manager_logs', 'compute_manager_log')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'record_comments', 'record_comment')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'service_dependencies', 'service_dependency')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'torsiondrive_optimizations', 'torsiondrive_optimization')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'torsiondrive_initial_molecules', 'torsiondrive_initial_molecule')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'gridoptimization_optimizations', 'gridoptimization_optimization')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'reaction_components', 'reaction_component')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'reaction_stoichiometries', 'reaction_stoichiometry')::json"
        )
    )

    # Others I missed before
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'kv_store', 'output_store')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, '\"dataset\"', '\"singlepoint_dataset\"')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, '\"dataset_entry\"', '\"singlepoint_dataset_entry\"')::json"
        )
    )


def downgrade():
    raise RuntimeError("Cannot downgrade")
