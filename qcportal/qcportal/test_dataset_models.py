from __future__ import annotations

from typing import TYPE_CHECKING
from qcportal.molecules import Molecule

import pytest

from qcfractal.components.singlepoint.testing_helpers import load_test_data, run_test_data
from qcportal import PortalRequestError
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.singlepoint import SinglepointDatasetNewEntry, SinglepointDataset

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_dataset_model_basic(submitter_client: PortalClient):
    ds = submitter_client.add_dataset(
        "optimization",
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
        "group1",
    )

    assert ds.dataset_type == "optimization"
    assert ds.name == "Test dataset"
    assert ds.description == "Test Description"
    assert ds.tagline == "a Tagline"
    assert ds.tags == ["tag1", "tag2"]
    assert ds.group == "new_group"
    assert ds.provenance == {"prov_key_1": "prov_value_1"}
    assert ds.visibility is True
    assert ds.default_tag == "def_tag"
    assert ds.default_priority == PriorityEnum.low

    assert ds.owner_user == submitter_client.username
    assert ds.owner_group == "group1"

    assert ds.entry_names == []


def test_dataset_model_metadata(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset(
        "optimization",
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

    # For name collision test
    snowflake_client.add_dataset("optimization", "Name collision")

    ds_id = ds.id

    ds.set_name("New dataset name")
    assert ds.name == "New dataset name"
    assert snowflake_client.get_dataset_by_id(ds_id).name == "New dataset name"

    # Name collision
    with pytest.raises(PortalRequestError, match="dataset named.*already exists"):
        ds.set_name("name cOLLision")
    assert ds.name == "New dataset name"

    ds.set_visibility(False)
    assert ds.visibility is False
    assert snowflake_client.get_dataset_by_id(ds_id).visibility is False
    ds.set_visibility(True)
    assert ds.visibility is True
    assert snowflake_client.get_dataset_by_id(ds_id).visibility is True

    ds.set_description("This is a new description")
    assert ds.description == "This is a new description"
    assert snowflake_client.get_dataset_by_id(ds_id).description == "This is a new description"

    ds.set_group("new_group")
    assert ds.group == "new_group"
    assert snowflake_client.get_dataset_by_id(ds_id).group == "new_group"

    ds.set_tags(["a_tag", "b_tag"])
    assert ds.tags == ["a_tag", "b_tag"]
    assert snowflake_client.get_dataset_by_id(ds_id).tags == ["a_tag", "b_tag"]

    ds.set_tagline("new ds tagline")
    assert ds.tagline == "new ds tagline"
    assert snowflake_client.get_dataset_by_id(ds_id).tagline == "new ds tagline"

    ds.set_provenance({"1": "hi"})
    assert ds.provenance == {"1": "hi"}
    assert snowflake_client.get_dataset_by_id(ds_id).provenance == {"1": "hi"}

    ds.set_metadata({"2": "hello"})
    assert ds.metadata == {"2": "hello"}
    assert snowflake_client.get_dataset_by_id(ds_id).metadata == {"2": "hello"}

    ds.set_default_tag("new_def_tag")
    assert ds.default_tag == "new_def_tag"
    assert snowflake_client.get_dataset_by_id(ds_id).default_tag == "new_def_tag"

    ds.set_default_priority(PriorityEnum.high)
    assert ds.default_priority == PriorityEnum.high
    assert snowflake_client.get_dataset_by_id(ds_id).default_priority == PriorityEnum.high


def test_dataset_model_status(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    run_test_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entries(SinglepointDatasetNewEntry(name="test_molecule", molecule=molecule))
    ds.add_entries(
        SinglepointDatasetNewEntry(name="test_molecule_2", molecule=molecule, additional_keywords={"maxiter": 999})
    )
    ds.submit()

    assert ds.status() == {"spec_1": {RecordStatusEnum.complete: 1, RecordStatusEnum.waiting: 1}}


def test_dataset_model_add_submit_many(snowflake_client: PortalClient):

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    mols = [Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, x]) for x in range(1, 3000)]
    entries = [SinglepointDatasetNewEntry(name=f"test_molecule_{idx}", molecule=m) for idx, m in enumerate(mols)]
    assert len(entries) == 2999

    meta = ds.add_entries(entries)
    assert meta.n_inserted == 2999
    assert meta.inserted_idx == list(range(2999))
    assert meta.n_existing == 0
    assert meta.existing_idx == []
    assert meta.n_errors == 0

    meta = ds.add_entries(entries)
    assert meta.n_inserted == 0
    assert meta.inserted_idx == []
    assert meta.n_existing == 2999
    assert meta.existing_idx == list(range(2999))
    assert meta.n_errors == 0

    ds.add_specification(
        "test_spec", specification={"program": "test_prog", "driver": "energy", "method": "HF", "basis": "sto-3g"}
    )
    ds.add_specification(
        "test_spec_2", specification={"program": "test_prog_2", "driver": "energy", "method": "HF", "basis": "sto-3g"}
    )

    ds.submit()

    assert ds.record_count == 2999 * 2
