from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal import PortalRequestError
from qcportal.records import PriorityEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


@pytest.mark.parametrize(
    "dataset_type", ["singlepoint", "optimization", "torsiondrive", "gridoptimization", "manybody", "reaction"]
)
def test_dataset_client_add_get(snowflake_client: PortalClient, dataset_type: str):

    ds = snowflake_client.add_dataset(
        dataset_type,
        "Test dataset",
        "Test Description",
        "a Tagline",
        ["tag1", "tag2"],
        "new_group",
        {"prov_key_1": "prov_value_1"},
        True,
        "def_tag",
        PriorityEnum.low,
        {"meta_key_1": "meta_value_1"},
    )

    assert ds.raw_data.dataset_type == dataset_type
    assert ds.raw_data.name == "Test dataset"
    assert ds.raw_data.description == "Test Description"
    assert ds.raw_data.tagline == "a Tagline"
    assert ds.raw_data.tags == ["tag1", "tag2"]
    assert ds.raw_data.group == "new_group"
    assert ds.raw_data.provenance == {"prov_key_1": "prov_value_1"}
    assert ds.raw_data.visibility is True
    assert ds.raw_data.default_tag == "def_tag"
    assert ds.raw_data.default_priority == PriorityEnum.low
    assert ds.raw_data.metadata == {"meta_key_1": "meta_value_1"}

    # case insensitive
    ds2 = snowflake_client.get_dataset(dataset_type, "test DATASET")
    assert ds2.raw_data == ds.raw_data


def test_dataset_client_add_same_name(snowflake_client: PortalClient):
    ds1 = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds2 = snowflake_client.add_dataset("optimization", "Test dataset")

    assert ds1.raw_data.id != ds2.raw_data.id


def test_dataset_client_add_duplicate(snowflake_client: PortalClient):
    snowflake_client.add_dataset("singlepoint", "Test dataset")

    with pytest.raises(PortalRequestError, match=r"Dataset.*already exists"):
        snowflake_client.add_dataset("singlepoint", "TEST DATASET")


def test_dataset_client_delete_empty(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_id = ds.raw_data.id

    ds = snowflake_client.get_dataset("singlepoint", "Test dataset")
    assert ds.raw_data.id == ds_id

    snowflake_client.delete_dataset(ds_id, False)

    with pytest.raises(PortalRequestError, match=r"Could not find all"):
        snowflake_client.get_dataset("singlepoint", "Test dataset")

    with pytest.raises(PortalRequestError, match=r"Could not find all"):
        snowflake_client.get_dataset_by_id(ds_id)


def test_dataset_client_query_dataset_records_1(snowflake_client: PortalClient):
    # Query which datasets contain a record
    raise RuntimeError("TODO")


def test_dataset_client_query_dataset_records_2(snowflake_client: PortalClient):
    # Query records that belong to a dataset
    raise RuntimeError("TODO")
