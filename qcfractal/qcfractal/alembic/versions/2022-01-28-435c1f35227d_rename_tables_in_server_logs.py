"""rename tables in server logs

Revision ID: 435c1f35227d
Revises: 1c01369d0bae
Create Date: 2022-01-28 15:50:56.612360

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "435c1f35227d"
down_revision = "1c01369d0bae"
branch_labels = None
depends_on = None


def upgrade():
    # Rename entries in the server stats log
    # (for simplicity, just do a text search and replace)
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'queue_manager', 'compute_manager')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'queue_manager_logs', 'compute_manager_logs')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'grid_optimization_association', 'gridoptimization_optimizations')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'grid_optimization_procedure', 'gridoptimization_record')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'kv_store', 'output_store')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'output_store', 'kv_store')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'service_queue_tasks', 'service_dependencies')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'optimization_history', 'torsiondrive_optimizations')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'torsiondrive_procedure', 'torsiondrive_record')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'torsion_init_mol_association', 'torsiondrive_initial_molecules')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'opt_result_association', 'optimization_trajectory')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'optimization_procedure', 'optimization_record')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'base_result', 'base_record')::json"
        )
    )
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'result', 'singlepoint_record')::json"
        )
    )


def downgrade():
    pass
