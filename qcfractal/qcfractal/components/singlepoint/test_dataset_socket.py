from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.exceptions import InvalidArgumentsError, MissingDataError

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_singlepoint_dataset_socket_entries_from_errors(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    src_opt_ds = snowflake_client.add_dataset("optimization", "Test src optimization dataset")
    src_sp_ds = snowflake_client.add_dataset("singlepoint", "Test src singlepoint dataset")

    sp_ds = snowflake_client.add_dataset("singlepoint", "Test singlepoint dataset")

    socket = snowflake.get_storage_socket()

    # Note - src_opt_ds.id + src_ds.id + sp_ds.id + 1 guaranteed not to exist
    missing_ds_id = src_opt_ds.id + src_sp_ds.id + sp_ds.id + 1

    # Destination dataset id does not exist
    with pytest.raises(MissingDataError, match="Cannot find dataset"):
        socket.datasets.singlepoint.add_entries_from_ds(
            dataset_id=missing_ds_id,
            from_dataset_id=src_opt_ds.id,
            from_dataset_type=None,
            from_dataset_name=None,
            from_specification_name="test_spec",
        )

    # Source dataset id does not exist
    with pytest.raises(MissingDataError, match="Cannot find dataset"):
        socket.datasets.singlepoint.add_entries_from_ds(
            dataset_id=sp_ds.id,
            from_dataset_id=missing_ds_id,
            from_dataset_type=None,
            from_dataset_name=None,
            from_specification_name="test_spec",
        )
    # Dataset type/name does not exist
    with pytest.raises(MissingDataError, match="Cannot find dataset"):
        socket.datasets.singlepoint.add_entries_from_ds(
            dataset_id=sp_ds.id,
            from_dataset_id=None,
            from_dataset_type="optimization",
            from_dataset_name="Does not exist",
            from_specification_name="test_spec",
        )

    # Bad dataset type for the given id
    with pytest.raises(InvalidArgumentsError, match="not singlepoint"):
        socket.datasets.singlepoint.add_entries_from_ds(
            dataset_id=sp_ds.id,
            from_dataset_id=src_opt_ds.id,
            from_dataset_type="singlepoint",
            from_dataset_name=None,
            from_specification_name="test_spec",
        )

    # Bad dataset type for the given id
    with pytest.raises(InvalidArgumentsError, match="not optimization"):
        socket.datasets.singlepoint.add_entries_from_ds(
            dataset_id=sp_ds.id,
            from_dataset_id=src_sp_ds.id,
            from_dataset_type="optimization",
            from_dataset_name=None,
            from_specification_name="test_spec",
        )

    # Optimization ds, but no specification
    with pytest.raises(InvalidArgumentsError, match="from_specification_name must be provided"):
        socket.datasets.singlepoint.add_entries_from_ds(
            dataset_id=sp_ds.id,
            from_dataset_id=src_opt_ds.id,
            from_dataset_type=None,
            from_dataset_name=None,
            from_specification_name=None,
        )
