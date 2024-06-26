"""rename manager tables

Revision ID: 01eb7ec0d803
Revises: cabe9df168a5
Create Date: 2021-11-02 09:26:51.278977

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import table, column

# revision identifiers, used by Alembic.
revision = "01eb7ec0d803"
down_revision = "cabe9df168a5"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###

    op.rename_table("queue_manager", "compute_manager")
    op.rename_table("queue_manager_logs", "compute_manager_logs")

    op.execute(sa.text("ALTER INDEX queue_manager_logs_pkey RENAME TO compute_manager_logs_pkey"))
    op.execute(sa.text("ALTER SEQUENCE queue_manager_logs_id_seq RENAME TO compute_manager_logs_id_seq"))

    op.execute(sa.text("ALTER INDEX queue_manager_pkey RENAME TO compute_manager_pkey"))
    op.execute(sa.text("ALTER SEQUENCE queue_manager_id_seq RENAME TO compute_manager_id_seq"))

    op.execute(sa.text("ALTER INDEX ix_queue_manager_modified_on RENAME TO ix_compute_manager_modified_on"))
    op.execute(sa.text("ALTER INDEX ix_queue_manager_status RENAME TO ix_compute_manager_status"))
    op.execute(sa.text("ALTER INDEX ix_queue_manager_log_timestamp RENAME TO ix_compute_manager_logs_timestamp"))

    op.execute(
        sa.text("ALTER TABLE compute_manager RENAME CONSTRAINT queue_manager_name_key TO ux_compute_manager_name")
    )
    op.execute(
        sa.text(
            "ALTER TABLE compute_manager_logs RENAME CONSTRAINT queue_manager_logs_manager_id_fkey TO compute_manager_logs_manager_id_fkey"
        )
    )

    # delete/rename some columns
    op.drop_column("compute_manager", "uuid")
    op.drop_column("compute_manager", "returned")

    # Add the rejected column
    op.add_column("compute_manager", sa.Column("rejected", sa.Integer(), nullable=True))

    # Make lots of columns not nullable
    op.execute(sa.text("UPDATE compute_manager SET completed = 0 WHERE completed IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET submitted = 0 WHERE submitted IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET failures = 0 WHERE failures IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET rejected = 0 WHERE rejected IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET total_worker_walltime = 0.0 WHERE total_worker_walltime IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET total_task_walltime = 0.0 WHERE total_task_walltime IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET active_tasks = 0 WHERE active_tasks IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET active_memory = 0.0 WHERE active_memory IS NULL"))
    op.execute(sa.text("UPDATE compute_manager SET active_cores = 0 WHERE active_cores IS NULL"))

    op.alter_column("compute_manager", "name", existing_type=sa.VARCHAR(), nullable=False)
    op.alter_column("compute_manager", "cluster", existing_type=sa.VARCHAR(), nullable=False)
    op.alter_column("compute_manager", "hostname", existing_type=sa.VARCHAR(), nullable=False)
    op.alter_column(
        "compute_manager", "completed", existing_type=sa.INTEGER(), nullable=False, new_column_name="successes"
    )
    op.alter_column(
        "compute_manager", "submitted", existing_type=sa.INTEGER(), nullable=False, new_column_name="claimed"
    )
    op.alter_column("compute_manager", "failures", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "compute_manager",
        "total_worker_walltime",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=False,
    )
    op.alter_column(
        "compute_manager",
        "total_task_walltime",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=False,
    )
    op.alter_column("compute_manager", "active_tasks", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("compute_manager", "active_cores", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "compute_manager", "active_memory", existing_type=postgresql.DOUBLE_PRECISION(precision=53), nullable=False
    )
    op.alter_column(
        "compute_manager",
        "status",
        existing_type=postgresql.ENUM("active", "inactive", name="managerstatusenum"),
        nullable=False,
    )
    op.alter_column("compute_manager", "created_on", existing_type=postgresql.TIMESTAMP(), nullable=False)
    op.alter_column("compute_manager", "modified_on", existing_type=postgresql.TIMESTAMP(), nullable=False)

    op.add_column("compute_manager_logs", sa.Column("rejected", sa.Integer(), nullable=True))
    op.execute(sa.text("UPDATE compute_manager_logs SET completed = 0 WHERE completed IS NULL"))
    op.execute(sa.text("UPDATE compute_manager_logs SET submitted = 0 WHERE submitted IS NULL"))
    op.execute(sa.text("UPDATE compute_manager_logs SET failures = 0 WHERE failures IS NULL"))
    op.execute(sa.text("UPDATE compute_manager_logs SET rejected = 0 WHERE rejected IS NULL"))
    op.execute(
        sa.text("UPDATE compute_manager_logs SET total_worker_walltime = 0.0 WHERE total_worker_walltime IS NULL")
    )
    op.execute(sa.text("UPDATE compute_manager_logs SET total_task_walltime = 0.0 WHERE total_task_walltime IS NULL"))
    op.execute(sa.text("UPDATE compute_manager_logs SET active_tasks = 0 WHERE active_tasks IS NULL"))
    op.execute(sa.text("UPDATE compute_manager_logs SET active_memory = 0.0 WHERE active_memory IS NULL"))
    op.execute(sa.text("UPDATE compute_manager_logs SET active_cores = 0 WHERE active_cores IS NULL"))
    op.alter_column(
        "compute_manager_logs", "completed", existing_type=sa.INTEGER(), nullable=False, new_column_name="successes"
    )
    op.alter_column(
        "compute_manager_logs", "submitted", existing_type=sa.INTEGER(), nullable=False, new_column_name="claimed"
    )
    op.alter_column("compute_manager_logs", "failures", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "compute_manager_logs",
        "total_worker_walltime",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=False,
    )
    op.alter_column(
        "compute_manager_logs",
        "total_task_walltime",
        existing_type=postgresql.DOUBLE_PRECISION(precision=53),
        nullable=False,
    )
    op.alter_column("compute_manager_logs", "active_tasks", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column("compute_manager_logs", "active_cores", existing_type=sa.INTEGER(), nullable=False)
    op.alter_column(
        "compute_manager_logs", "active_memory", existing_type=postgresql.DOUBLE_PRECISION(precision=53), nullable=False
    )

    # Migrate tags
    op.add_column("compute_manager", sa.Column("tags", postgresql.ARRAY(sa.String())))

    # tag was originally a string
    # So trim any curly braces and split by comma, forming an array
    op.execute(
        sa.text(
            r"UPDATE compute_manager SET tags = regexp_split_to_array(trim(BOTH '{}' FROM LOWER(tag)), '\s*,\s*') WHERE tag IS NOT NULL"
        )
    )

    # now set previously null tags to wildcard
    op.execute(sa.text(r"UPDATE compute_manager SET tags = ARRAY['*'] WHERE tag IS NULL"))

    # Now make column nullable
    op.alter_column(
        "compute_manager",
        "tags",
        nullable=False,
    )

    op.alter_column(
        "compute_manager",
        "rejected",
        nullable=False,
    )

    op.alter_column(
        "compute_manager_logs",
        "rejected",
        nullable=False,
    )

    # Deal with null manager and qcengine version
    op.execute(sa.text(r"UPDATE compute_manager SET manager_version = 'v0' WHERE manager_version IS NULL"))

    op.execute(sa.text(r"UPDATE compute_manager SET qcengine_version = 'v0' WHERE qcengine_version IS NULL"))

    op.execute(sa.text(r"UPDATE compute_manager SET programs = '{}'::json WHERE programs IS NULL"))

    op.alter_column("compute_manager", "qcengine_version", nullable=False)
    op.alter_column("compute_manager", "manager_version", nullable=False)
    op.alter_column("compute_manager", "programs", nullable=False)

    op.drop_column("compute_manager", "tag")
    op.drop_column("compute_manager", "configuration")

    # Convert programs from list to dictionary
    programs_helper = table("compute_manager", column("id", sa.Integer), column("programs", sa.JSON))

    bind = op.get_bind()
    session = Session(bind=bind)

    for p in session.query(programs_helper).yield_per(500):
        if isinstance(p.programs, list):
            new_progs = {k.lower(): None for k in p.programs}
            session.execute(
                programs_helper.update().where(programs_helper.c.id == p.id).values({"programs": new_progs})
            )

    # ### end Alembic commands ###


def downgrade():
    raise RuntimeError("Cannot downgrade")
