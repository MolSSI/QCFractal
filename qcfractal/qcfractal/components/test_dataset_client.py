from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pytest

from qcfractal.components.singlepoint.testing_helpers import load_test_data, run_test_data
from qcportal import PortalRequestError
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint import SinglepointDataset

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcarchivetesting.testing_classes import QCATestingSnowflake


@pytest.mark.parametrize(
    "dataset_type", ["singlepoint", "optimization", "torsiondrive", "gridoptimization", "manybody", "reaction", "neb"]
)
@pytest.mark.parametrize("owner_group", ["group1", None])
def test_dataset_client_add_get(submitter_client: PortalClient, dataset_type: str, owner_group: Optional[str]):
    ds = submitter_client.add_dataset(
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
        owner_group=owner_group,
    )

    assert ds.dataset_type == dataset_type
    assert ds.name == "Test dataset"
    assert ds.description == "Test Description"
    assert ds.tagline == "a Tagline"
    assert ds.tags == ["tag1", "tag2"]
    assert ds.provenance == {"prov_key_1": "prov_value_1"}
    assert ds.default_compute_tag == "def_tag"
    assert ds.default_compute_priority == PriorityEnum.low
    assert ds.extras == {"meta_key_1": "meta_value_1"}

    assert ds.owner_user == submitter_client.username
    assert ds.owner_group == owner_group

    # case insensitive
    ds2 = submitter_client.get_dataset(dataset_type, "test DATASET")
    assert ds2.id == ds.id


def test_dataset_client_status(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    run_test_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)
    ds.submit()

    stat1 = ds.status()
    stat2 = snowflake_client.get_dataset_status_by_id(ds.id)
    assert stat1 == stat2


def test_dataset_client_add_same_name(snowflake_client: PortalClient):
    ds1 = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds2 = snowflake_client.add_dataset("optimization", "Test dataset")

    assert ds1.id != ds2.id


def test_dataset_client_add_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")

    with pytest.raises(PortalRequestError, match=r"Dataset.*already exists"):
        # existing_ok = False by default
        snowflake_client.add_dataset("singlepoint", "TEST DATASET")

    ds2 = snowflake_client.add_dataset("singlepoint", "Test dataset", existing_ok=True)
    assert ds.id == ds2.id


def test_dataset_client_delete_empty(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_id = ds.id

    ds = snowflake_client.get_dataset("singlepoint", "Test dataset")
    assert ds.id == ds_id

    snowflake_client.delete_dataset(ds_id, False)

    with pytest.raises(PortalRequestError, match=r"Could not find singlepoint dataset with name"):
        snowflake_client.get_dataset("singlepoint", "Test dataset")

    with pytest.raises(PortalRequestError, match=r"Could not find dataset with id"):
        snowflake_client.get_dataset_by_id(ds_id)

    all_ds = snowflake_client.list_datasets()
    len(all_ds) == 0


def test_dataset_client_query_dataset_records(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    run_test_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)
    ds.submit()

    # Query records belonging to a dataset
    rec_id_2 = ds.get_record("test_molecule_2", "spec_1").id
    mol_id_2 = ds.get_entry("test_molecule_2").molecule.id

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id)
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id, molecule_id=mol_id_2)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1
    assert query_res_l[0].id == rec_id_2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id + 1, molecule_id=mol_id_2)
    assert query_res._fetched == 0

    # Query which dataset contains a record
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 1
    assert rec_info[0]["dataset_id"] == 1
    assert rec_info[0]["entry_name"] == "test_molecule_2"

    # Query which dataset contains a record
    ds.remove_records(entry_names="test_molecule_2", specification_names="spec_1", delete_records=True)
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 0


def test_dataset_rename(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.name == "Test dataset"
    ds_id = ds.id
    assert ds.status() == {}

    ds.set_name("Different name")
    ds = snowflake_client.get_dataset_by_id(ds_id)
    assert ds.name == "Different name"

    ds.set_name("different name")
    ds = snowflake_client.get_dataset_by_id(ds_id)
    assert ds.name == "different name"


def test_dataset_rename_specifications(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    ds.add_specification("spec_1", input_spec)

    assert len(ds.specifications) == 1

    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert len(ds.specifications) == 1
    ds.rename_specification("spec_1", "spec_2")

    assert "spec_2" in ds.specifications
    assert "spec_1" not in ds.specifications

    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert "spec_2" in ds.specifications
    assert "spec_1" not in ds.specifications

    # Rename without actually renaming
    ds.rename_specification("spec_2", "spec_2")
    assert "spec_2" in ds.specifications
    assert "spec_1" not in ds.specifications

    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert "spec_2" in ds.specifications
    assert "spec_1" not in ds.specifications


def test_dataset_rename_entries(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])

    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)

    assert len(ds.entry_names) == 2

    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert len(ds.entry_names) == 2
    ds.rename_entries({"test_molecule": "different_name"})

    assert "different_name" in ds.entry_names
    assert "test_molecule" not in ds.entry_names
    assert "test_molecule_2" in ds.entry_names

    ds.rename_entries({"test_molecule_2": "test_molecule_2"})

    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert len(ds.entry_names) == 2

    assert "different_name" in ds.entry_names
    assert "test_molecule" not in ds.entry_names
    assert "test_molecule_2" in ds.entry_names


def test_dataset_client_get_computed_properties(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    record_id = run_test_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.submit()

    assert ds.status()["spec_1"]["complete"] == 1

    computed_prop = ds.computed_properties
    assert "spec_1" in computed_prop
    assert "scf_total_energy" in computed_prop["spec_1"]
    assert "calcinfo_natom" in computed_prop["spec_1"]


def test_dataset_client_copy_from_incompatible(snowflake_client: PortalClient):
    ds_1 = snowflake_client.add_dataset("singlepoint", "Test sp dataset")
    ds_2 = snowflake_client.add_dataset("optimization", "Test opt dataset")

    with pytest.raises(PortalRequestError, match="does not match destination type"):
        ds_2.copy_records_from(ds_1.id)
