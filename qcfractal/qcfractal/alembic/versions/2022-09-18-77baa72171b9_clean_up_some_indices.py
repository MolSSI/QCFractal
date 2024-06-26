"""Clean up some indices

Revision ID: 77baa72171b9
Revises: 64d8c7cc3a18
Create Date: 2022-09-18 09:57:51.222000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "77baa72171b9"
down_revision = "64d8c7cc3a18"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index("ix_access_log_access_type", table_name="access_log")
    op.drop_index("ix_native_file_record_id", table_name="native_file")
    op.drop_index("ix_output_store_history_id", table_name="output_store")
    op.drop_index("ix_record_compute_history_manager_name", table_name="record_compute_history")

    op.drop_index("ix_dataset_type", table_name="base_dataset")
    op.create_index("ix_base_dataset_dataset_type", "base_dataset", ["dataset_type"], unique=False)
    op.drop_constraint("uix_dataset_type_lname", "base_dataset", type_="unique")
    op.create_unique_constraint("ux_base_dataset_dataset_type_lname", "base_dataset", ["dataset_type", "lname"])

    op.create_index("ix_contributed_values_dataset_id", "contributed_values", ["dataset_id"], unique=False)

    # Change some index types
    op.drop_index("ix_access_log_access_date", table_name="access_log")
    op.create_index("ix_access_log_access_date", "access_log", ["access_date"], postgresql_using="brin")

    op.drop_index("ix_internal_error_log_error_date", table_name="internal_error_log")
    op.create_index("ix_internal_error_log_error_date", "internal_error_log", ["error_date"], postgresql_using="brin")

    op.drop_index("ix_server_stats_log_timestamp", table_name="server_stats_log")
    op.create_index("ix_server_stats_log_timestamp", "server_stats_log", ["timestamp"], postgresql_using="brin")

    op.drop_index("ix_molecule_hash", table_name="molecule")
    op.create_index("ix_molecule_molecule_hash", "molecule", ["molecule_hash"], postgresql_using="hash")

    op.drop_index("ix_compute_manager_log_timestamp", table_name="compute_manager")
    op.create_index("ix_compute_manager_log_timestamp", "compute_manager_log", ["timestamp"], postgresql_using="brin")

    op.drop_index("ix_compute_manager_modified_on", table_name="compute_manager_log")
    op.create_index("ix_compute_manager_modified_on", "compute_manager", ["modified_on"], postgresql_using="brin")

    op.drop_index("ix_internal_jobs_added_date", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_scheduled_date", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_last_updated", table_name="internal_jobs")
    op.create_index("ix_internal_jobs_added_date", "internal_jobs", ["added_date"], postgresql_using="brin")
    op.create_index("ix_internal_jobs_scheduled_date", "internal_jobs", ["scheduled_date"], postgresql_using="brin")
    op.create_index("ix_internal_jobs_last_updated", "internal_jobs", ["last_updated"], postgresql_using="brin")

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index("ix_record_compute_history_manager_name", "record_compute_history", ["manager_name"], unique=False)
    op.create_index("ix_output_store_history_id", "output_store", ["history_id"], unique=False)
    op.create_index("ix_native_file_record_id", "native_file", ["record_id"], unique=False)
    op.create_index("ix_access_log_access_type", "access_log", ["access_type"], unique=False)

    op.drop_constraint("ux_base_dataset_dataset_type_lname", "base_dataset", type_="unique")
    op.drop_index("ix_base_dataset_dataset_type", table_name="base_dataset")
    op.create_unique_constraint("uix_dataset_type_lname", "base_dataset", ["dataset_type", "lname"])
    op.create_index("ix_dataset_type", "base_dataset", ["dataset_type"], unique=False)

    op.drop_index("ix_contributed_values_dataset_id", table_name="contributed_values")

    op.drop_index("ix_access_log_access_date", table_name="access_log")
    op.create_index("ix_access_log_access_date", "access_log", ["access_date"])

    op.drop_index("ix_internal_error_log_error_date", table_name="internal_error_log")
    op.create_index("ix_internal_error_log_error_date", "internal_error_log", ["error_date"])

    op.drop_index("ix_server_stats_log_timestamp", table_name="server_stats_log")
    op.create_index("ix_server_stats_log_timestamp", "server_stats_log", ["timestamp"])

    op.drop_index("ix_molecule_molecule_hash", table_name="molecule")
    op.create_index("ix_molecule_hash", "molecule", ["molecule_hash"])

    op.drop_index("ix_compute_manager_log_timestamp", table_name="compute_manager")
    op.create_index("ix_compute_manager_log_timestamp", "compute_manager_log", ["timestamp"])

    op.drop_index("ix_compute_manager_modified_on", table_name="compute_manager_log")
    op.create_index("ix_compute_manager_modified_on", "compute_manager", ["modified_on"])

    op.drop_index("ix_internal_jobs_added_date", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_scheduled_date", table_name="internal_jobs")
    op.drop_index("ix_internal_jobs_last_updated", table_name="internal_jobs")
    op.create_index("ix_internal_jobs_added_date", "internal_jobs", ["added_date"])
    op.create_index("ix_internal_jobs_scheduled_date", "internal_jobs", ["scheduled_date"])
    op.create_index("ix_internal_jobs_last_updated", "internal_jobs", ["last_updated"])

    # ### end Alembic commands ###
