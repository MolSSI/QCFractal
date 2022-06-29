from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.records.singlepoint.testing_helpers import load_test_data, run_test_data
from qcportal import PortalRequestError
from qcportal.datasets.singlepoint import SinglepointDatasetNewEntry, SinglepointDataset
from qcportal.molecules import Molecule
from qcportal.records import PriorityEnum

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcportal.managers import ManagerName
    from qcfractal.db_socket import SQLAlchemySocket


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


def test_dataset_client_query_dataset_records(
    storage_socket: SQLAlchemySocket, snowflake_client: PortalClient, activated_manager_name: ManagerName
):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    run_test_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")

    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entries(SinglepointDatasetNewEntry(name="test_molecule", molecule=molecule))
    ds.add_entries(SinglepointDatasetNewEntry(name="test_molecule_2", molecule=molecule_2))
    ds.submit()

    # Query records belonging to a dataset
    rec_id_2 = ds.get_record("test_molecule_2", "spec_1").id
    mol_id_2 = ds.get_entry("test_molecule_2").molecule.id

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id)
    assert query_res.current_meta.n_found == 2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id, molecule_id=mol_id_2)
    assert query_res.current_meta.n_found == 1
    assert list(query_res)[0].id == rec_id_2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id + 1, molecule_id=mol_id_2)
    assert query_res.current_meta.n_found == 0

    # Query which dataset contains a record
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 1
    assert rec_info[0]["dataset_id"] == 1
    assert rec_info[0]["entry_name"] == "test_molecule_2"

    # Query which dataset contains a record
    ds.remove_records(entry_names="test_molecule_2", specification_names="spec_1", delete_records=True)
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 0
