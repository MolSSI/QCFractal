"""add compute history

Revision ID: 88e6b7d536d0
Revises: dd0121101dc2
Create Date: 2021-11-11 09:10:10.766466

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import declarative_base

Base = declarative_base()

# revision identifiers, used by Alembic.
revision = "88e6b7d536d0"
down_revision = "dd0121101dc2"
branch_labels = None
depends_on = None

status_enum = postgresql.ENUM(
    "complete", "waiting", "running", "error", "cancelled", "deleted", name="recordstatusenum", create_type=False
)


def upgrade():
    # Create the compute history table
    op.create_table(
        "record_compute_history",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("record_id", sa.Integer(), nullable=False),
        sa.Column("status", status_enum, nullable=False),
        sa.Column("manager_name", sa.String(), nullable=True),
        sa.Column("modified_on", sa.DateTime(), nullable=False),
        sa.Column("provenance", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["record_id"], ["base_record.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["manager_name"], ["compute_manager.name"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_record_compute_history_record_id", "record_compute_history", ["record_id"], unique=False)
    op.create_index("ix_record_compute_history_manager_name", "record_compute_history", ["manager_name"], unique=False)

    # Add columns for record history and output type to the output store
    # nullable for now, will change later in this migration
    op.add_column("output_store", sa.Column("history_id", sa.Integer(), nullable=True))
    op.create_foreign_key(None, "output_store", "record_compute_history", ["history_id"], ["id"], ondelete="CASCADE")
    op.create_index("ix_output_store_history_id", "output_store", ["history_id"])

    output_enum = postgresql.ENUM("stdout", "stderr", "error", name="outputtypeenum")
    output_enum.create(op.get_bind())
    op.add_column("output_store", sa.Column("output_type", output_enum, nullable=True))
    op.create_unique_constraint("ux_output_store_id_type", "output_store", ["history_id", "output_type"])

    #####################################
    # Populate the compute history table
    # Any computations with status = complete or status = error
    op.execute(
        sa.text(
            """
               INSERT INTO record_compute_history (record_id,status,manager_name,modified_on,provenance)
               SELECT id,status,manager_name,modified_on,provenance FROM base_record
               WHERE base_record.status IN ('complete', 'error')
               """
        )
    )

    # Other status with a provenance, manager, output, etc are previously errored calculations
    # that were reset and are now waiting, running, cancelled
    # The base record contains managers that actually attempted to run the calculation. The
    # task queue entry contains what is currently running it
    # modified_on may be incorrect, but it is the best we can do
    op.execute(
        sa.text(
            """
               INSERT INTO record_compute_history (record_id,status,manager_name,modified_on,provenance)
               SELECT id,'error',manager_name,modified_on,provenance FROM base_record
               WHERE base_record.status NOT IN ('complete', 'error')
               AND (base_record.provenance IS NOT NULL
               OR base_record.stdout IS NOT NULL
               OR base_record.stderr IS NOT NULL
               OR base_record.error IS NOT NULL)
               """
        )
    )

    # Copy over the manager from the task queue. Now, we want the base_record manager to
    # reflect the currently-assigned manager
    op.execute(
        sa.text(
            """
               UPDATE base_record
               SET manager_name = task_queue.manager
               FROM task_queue
               WHERE base_record.id = task_queue.record_id
               AND base_record.status = 'running';
               """
        )
    )

    # Now update the output store with foreign keys to history records
    op.execute(
        sa.text(
            """UPDATE output_store SET output_type='stdout', history_id = record_compute_history.id
               FROM base_record
               INNER JOIN record_compute_history ON record_compute_history.record_id = base_record.id
               WHERE base_record.stdout = output_store.id;
               """
        )
    )
    op.execute(
        sa.text(
            """UPDATE output_store SET output_type='stderr', history_id = record_compute_history.id
               FROM base_record
               INNER JOIN record_compute_history ON record_compute_history.record_id = base_record.id
               WHERE base_record.stderr = output_store.id;
               """
        )
    )
    op.execute(
        sa.text(
            """UPDATE output_store SET output_type='error', history_id = record_compute_history.id
               FROM base_record
               INNER JOIN record_compute_history ON record_compute_history.record_id = base_record.id
               WHERE base_record.error = output_store.id;
               """
        )
    )

    # Delete everything from the outputstore that is not linked
    # We do this before dropping the columns on base_record. This will prevent any lost
    # data in output_store in case I screwed up the commands above, and there is data linked to from
    # base_record that hasn't been migrated
    # (since deleting an output_store row that is in-use will cause an exception).
    op.execute(sa.text("DELETE FROM output_store WHERE history_id IS NULL"))

    # Now we can safely delete the columns/indices/constraints in base_record
    op.drop_constraint("base_result_stdout_fkey", "base_record", type_="foreignkey")
    op.drop_constraint("base_result_error_fkey", "base_record", type_="foreignkey")
    op.drop_constraint("base_result_stderr_fkey", "base_record", type_="foreignkey")
    op.drop_index("ix_base_result_stdout", table_name="base_record")
    op.drop_index("ix_base_result_stderr", table_name="base_record")
    op.drop_index("ix_base_result_error", table_name="base_record")
    op.drop_column("base_record", "error")
    op.drop_column("base_record", "stdout")
    op.drop_column("base_record", "stderr")
    op.drop_column("base_record", "provenance")

    # Now we can drop the manager info from the task queue that has been hanging around for a while
    op.drop_index("ix_task_queue_manager", table_name="task_queue")
    op.drop_constraint("task_queue_manager_fkey", "task_queue", type_="foreignkey")
    op.drop_column("task_queue", "manager")

    # Now make stuff not nullable
    op.alter_column("output_store", "history_id", nullable=False)
    op.alter_column("output_store", "output_type", nullable=False)

    # ### end Alembic commands ###


def downgrade():
    raise RuntimeError("Cannot downgrade")
