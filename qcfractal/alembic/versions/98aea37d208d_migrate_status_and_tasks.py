"""migrate_status_and_tasks

Revision ID: 98aea37d208d
Revises: 88182596f844
Create Date: 2021-06-25 09:34:07.378008

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func, column
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "98aea37d208d"
down_revision = "88182596f844"
branch_labels = None
depends_on = None


def upgrade():
    # Queue manager
    op.drop_column("queue_manager", "procedures")

    # Migrate base_result status to new enum, taking into account task_queue and service_queue statuses
    old_enum = sa.Enum("incomplete", "complete", "running", "error", name="recordstatusenum")
    new_enum = sa.Enum(
        "complete", "waiting", "running", "error", "invalid", "cancelled", "deleted", name="recordstatusenum"
    )

    # For the temporary, we only need to add "waiting" and "cancelled", then migrate incomplete records with
    # task status of 'waiting' to 'waiting' (and running -> running)
    tmp_enum = sa.Enum("incomplete", "complete", "running", "error", "waiting", "cancelled", name="_statustmp")

    # Alter the existing base_result table to use the temporary enum and then drop the old one
    tmp_enum.create(op.get_bind(), checkfirst=False)
    op.execute("ALTER TABLE base_result ALTER COLUMN status TYPE _statustmp USING status::text::_statustmp")
    old_enum.drop(op.get_bind(), checkfirst=False)

    # Now migrate the status in base result to reflect any status in the task/service queues
    # The only thing to really change is 'incomplete' base_result rows. Change those to be the status
    # of the task/service)
    op.execute(
        "UPDATE base_result SET status = task_queue.status::text::_statustmp FROM task_queue WHERE base_result.id = task_queue.base_result_id AND base_result.status = 'incomplete'"
    )
    op.execute(
        "UPDATE base_result SET status = service_queue.status::text::_statustmp FROM service_queue WHERE base_result.id = service_queue.procedure_id AND base_result.status = 'incomplete'"
    )

    # Delete all tasks/services for completed records
    op.execute(
        """
               DELETE FROM task_queue
               USING base_result
               WHERE base_result.id = task_queue.base_result_id
               AND base_result.status = 'complete'
               """
    )

    # Delete all tasks/services for completed records
    op.execute(
        """
               DELETE FROM service_queue
               USING base_result
               WHERE base_result.id = service_queue.procedure_id
               AND base_result.status = 'complete'
               """
    )

    # Also mark all results without a corresponding task or service as "cancelled"
    op.execute(
        """
               UPDATE base_result
               SET status='cancelled'
               WHERE id IN (
                   SELECT base_result.id FROM base_result
                   LEFT OUTER JOIN task_queue ON task_queue.base_result_id = base_result.id
                   WHERE base_result.status != 'complete'
                   AND base_result.result_type IN ('result', 'optimization_procedure')
                   AND task_queue.base_result_id IS NULL
               )
               """
    )

    op.execute(
        """
               UPDATE base_result
               SET status='cancelled'
               WHERE id IN (
                   SELECT base_result.id FROM base_result
                   LEFT OUTER JOIN service_queue ON service_queue.procedure_id = base_result.id
                   WHERE base_result.status != 'complete'
                   AND base_result.result_type IN ('torsiondrive_procedure', 'gridoptimization_procedure')
                   AND service_queue.procedure_id IS NULL
               )
               """
    )

    # Reset tasks that are assigned to inactive managers
    op.execute(
        """
               UPDATE base_result
               SET status='waiting'
               WHERE id IN (
                   SELECT base_result.id FROM base_result
                   LEFT OUTER JOIN queue_manager ON base_result.manager_name = queue_manager.name
                   WHERE base_result.status = 'running'
                   AND base_result.result_type IN ('result', 'optimization_procedure')
                   AND queue_manager.status = 'inactive'
               )
               """
    )

    # Now alter base_result to the new status enum, now that all the incompletes are removed
    new_enum.create(op.get_bind(), checkfirst=False)
    op.execute("ALTER TABLE base_result ALTER COLUMN status TYPE recordstatusenum USING status::text::recordstatusenum")
    tmp_enum.drop(op.get_bind(), checkfirst=False)

    # Now do the required_programs column of the task_queue
    # Form this from the program and procedure columns
    op.add_column("task_queue", sa.Column("required_programs", postgresql.ARRAY(sa.TEXT()), nullable=True))
    op.execute("UPDATE task_queue SET required_programs = ARRAY[program, procedure] WHERE procedure IS NOT NULL")
    op.execute("UPDATE task_queue SET required_programs = ARRAY[program] WHERE procedure IS NULL")
    op.alter_column("task_queue", "required_programs", existing_type=postgresql.JSONB, nullable=False)
    op.create_check_constraint(
        "ck_task_queue_requirements_lower",
        "task_queue",
        column("required_programs").cast(sa.TEXT) == func.lower(column("required_programs").cast(sa.TEXT)),
    )

    # OTHER MANIPULATIONS BELOW
    # Task Queue
    # NOTE: We are leaving the manager column in place for a future migration
    op.drop_index("ix_task_queue_created_on", table_name="task_queue")
    op.drop_index("ix_task_queue_keys", table_name="task_queue")
    op.drop_index("ix_task_waiting_sort", table_name="task_queue")

    op.create_index("ix_task_queue_required_programs", "task_queue", ["required_programs"], unique=False)
    op.create_index("ix_task_queue_tag", "task_queue", ["tag"], unique=False)
    op.execute("CREATE INDEX ix_task_queue_waiting_sort ON task_queue (priority desc, created_on)")

    op.drop_column("task_queue", "modified_on")
    op.drop_column("task_queue", "procedure")
    op.drop_column("task_queue", "status")
    op.drop_column("task_queue", "parser")
    op.drop_column("task_queue", "program")

    op.alter_column("task_queue", "created_on", nullable=False)
    op.alter_column("task_queue", "priority", nullable=False)

    # Service Queue
    op.drop_index("ix_service_queue_modified_on", table_name="service_queue")
    op.drop_index("ix_service_queue_priority", table_name="service_queue")
    op.drop_index("ix_service_queue_status", table_name="service_queue")
    op.drop_index("ix_service_queue_status_tag_hash", table_name="service_queue")
    op.create_index("ix_service_queue_tag", "service_queue", ["tag"], unique=False)
    op.execute("CREATE INDEX ix_service_queue_waiting_sort ON service_queue (priority desc, created_on)")
    op.drop_column("service_queue", "status")
    # ### end Alembic commands ###


def downgrade():
    raise RuntimeError("Cannot downgrade")

    # ### commands auto generated by Alembic - please adjust! ###
    # op.add_column('task_queue', sa.Column('program', sa.VARCHAR(), autoincrement=False, nullable=True))
    # op.add_column('task_queue', sa.Column('parser', sa.VARCHAR(), autoincrement=False, nullable=True))
    # op.add_column('task_queue', sa.Column('status', postgresql.ENUM('running', 'waiting', 'error', 'complete', name='taskstatusenum'), autoincrement=False, nullable=True))
    # op.add_column('task_queue', sa.Column('manager', sa.VARCHAR(), autoincrement=False, nullable=True))
    # op.add_column('task_queue', sa.Column('procedure', sa.VARCHAR(), autoincrement=False, nullable=True))
    # op.add_column('task_queue', sa.Column('modified_on', postgresql.TIMESTAMP(), autoincrement=False, nullable=True))
    # op.create_foreign_key('task_queue_manager_fkey', 'task_queue', 'queue_manager', ['manager'], ['name'], ondelete='SET NULL')
    # op.drop_index('ix_task_queue_tag', table_name='task_queue')
    # op.drop_index('ix_task_queue_required_programs', table_name='task_queue')
    # op.create_index('ix_task_waiting_sort', 'task_queue', ['priority', 'created_on'], unique=False)
    # op.create_index('ix_task_queue_manager', 'task_queue', ['manager'], unique=False)
    # op.create_index('ix_task_queue_keys', 'task_queue', ['status', 'program', 'procedure', 'tag'], unique=False)
    # op.create_index('ix_task_queue_created_on', 'task_queue', ['created_on'], unique=False)
    # op.drop_column('task_queue', 'required_programs')
    # op.add_column('service_queue', sa.Column('status', postgresql.ENUM('running', 'waiting', 'error', 'complete', name='taskstatusenum'), autoincrement=False, nullable=True))
    # op.drop_index('ix_service_queue_tag', table_name='service_queue')
    # op.create_index('ix_service_queue_status_tag_hash', 'service_queue', ['status', 'tag'], unique=False)
    # op.create_index('ix_service_queue_status', 'service_queue', ['status'], unique=False)
    # op.create_index('ix_service_queue_priority', 'service_queue', ['priority'], unique=False)
    # op.create_index('ix_service_queue_modified_on', 'service_queue', ['modified_on'], unique=False)
    # op.add_column('queue_manager', sa.Column('procedures', postgresql.JSON(astext_type=sa.Text()), autoincrement=False, nullable=True))
    # ### end Alembic commands ###
