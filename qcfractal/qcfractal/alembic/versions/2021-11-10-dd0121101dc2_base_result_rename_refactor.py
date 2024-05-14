"""Renaming of base_result

Revision ID: dd0121101dc2
Revises: 01eb7ec0d803
Create Date: 2021-11-10 12:45:53.662253

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "dd0121101dc2"
down_revision = "01eb7ec0d803"
branch_labels = None
depends_on = None


def upgrade():
    # Drop unused version & hash index columns
    op.drop_column("base_result", "version")
    op.drop_column("base_result", "hash_index")

    ##################################################
    # result-type column is now record type
    # We mostly want what is in the procedure column to be the polymorphic column
    # for inheritance
    op.drop_index("ix_base_result_type", table_name="base_result")
    op.drop_column("base_result", "result_type")
    op.alter_column("base_result", "procedure", new_column_name="record_type", nullable=False)
    op.execute(sa.text("UPDATE base_result SET record_type = 'singlepoint' WHERE record_type = 'single'"))
    op.create_index("ix_base_record_record_type", "base_result", ["record_type"], unique=False)

    ##################################################
    # Rename the status index
    op.execute(sa.text("ALTER INDEX ix_base_result_status RENAME TO ix_base_record_status"))

    ##################################################
    # Now rename the whole table, and the primary key index
    op.rename_table("base_result", "base_record")
    op.execute(sa.text("ALTER SEQUENCE base_result_id_seq RENAME TO base_record_id_seq"))
    op.execute(sa.text("ALTER INDEX base_result_pkey RENAME TO base_record_pkey"))

    ################################################################################
    # Rename base_result/result/procedure to record in task queue
    op.drop_constraint("task_queue_base_result_id_key", "task_queue", type_="unique")
    op.drop_index("ix_task_queue_base_result_id", table_name="task_queue")
    op.alter_column("task_queue", "base_result_id", new_column_name="record_id")
    op.create_unique_constraint("ux_task_queue_record_id", "task_queue", ["record_id"])
    op.execute(
        sa.text("ALTER TABLE task_queue RENAME CONSTRAINT task_queue_base_result_id_fkey TO task_queue_record_id_fkey")
    )

    ################################################################################
    # Same for service queue
    op.drop_constraint("service_queue_procedure_id_key", "service_queue", type_="unique")
    op.alter_column("service_queue", "procedure_id", new_column_name="record_id")
    op.execute(
        sa.text(
            "ALTER TABLE service_queue RENAME CONSTRAINT service_queue_procedure_id_fkey TO service_queue_record_id_fkey"
        )
    )
    op.create_unique_constraint("ux_service_queue_record_id", "service_queue", ["record_id"])

    # And the service queue tasks table
    # There, we have to recreate the primary key
    op.alter_column("service_queue_tasks", "procedure_id", new_column_name="record_id")
    op.execute(
        sa.text(
            "ALTER TABLE service_queue_tasks RENAME CONSTRAINT service_queue_tasks_procedure_id_fkey TO service_queue_tasks_record_id_fkey"
        )
    )

    ##################################################
    # Rename the foreign key constraint to manager name
    # (we drop & recreate because ondelete also changed
    op.drop_constraint("base_result_manager_name_fkey", "base_record", type_="foreignkey")
    op.create_foreign_key("base_record_manager_name_fkey", "base_record", "compute_manager", ["manager_name"], ["name"])
    op.create_index("ix_base_record_manager_name", "base_record", ["manager_name"], unique=False)

    # Create an index on protocols
    # This will be used in later migrations, then the protocols column will be removed
    op.create_index("ix_base_record_protocols", "base_record", ["protocols"], unique=False, postgresql_using="gin")

    ################################################################################
    # Rename columns in server information
    op.alter_column("server_stats_log", "result_count", new_column_name="record_count")

    # (for simplicity, just do a text search and replace inside the JSON fields)
    op.execute(
        sa.text(
            "UPDATE server_stats_log SET db_table_information = replace(db_table_information::text, 'base_result', 'base_record')::json"
        )
    )

    op.execute(
        sa.text(
            "UPDATE server_stats_log SET task_queue_status = replace(task_queue_status::text, 'result_type', 'record_type')::json"
        )
    )

    op.execute(
        sa.text(
            "UPDATE server_stats_log SET task_queue_status = replace(task_queue_status::text, 'single', 'singlepoint')::json"
        )
    )

    op.execute(
        sa.text(
            "UPDATE server_stats_log SET service_queue_status = replace(service_queue_status::text, 'result_type', 'record_type')::json"
        )
    )

    # Some columns not nullable
    op.alter_column("base_record", "created_on", nullable=False)
    op.alter_column("base_record", "modified_on", nullable=False)


def downgrade():
    pass
