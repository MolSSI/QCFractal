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
    from qcportal.managers import ManagerName
    from qcfractal.db_socket import SQLAlchemySocket


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
    assert ds.group == "new_group"
    assert ds.provenance == {"prov_key_1": "prov_value_1"}
    assert ds.visibility is True
    assert ds.default_tag == "def_tag"
    assert ds.default_priority == PriorityEnum.low
    assert ds.metadata == {"meta_key_1": "meta_value_1"}

    assert ds.owner_user == submitter_client.username
    assert ds.owner_group == owner_group

    # case insensitive
    ds2 = submitter_client.get_dataset(dataset_type, "test DATASET")
    assert ds2.id == ds.id


def test_dataset_client_add_same_name(snowflake_client: PortalClient):
    ds1 = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds2 = snowflake_client.add_dataset("optimization", "Test dataset")

    assert ds1.id != ds2.id


def test_dataset_client_add_duplicate(snowflake_client: PortalClient):
    snowflake_client.add_dataset("singlepoint", "Test dataset")

    with pytest.raises(PortalRequestError, match=r"Dataset.*already exists"):
        snowflake_client.add_dataset("singlepoint", "TEST DATASET")


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
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)
    ds.submit()

    # Query records belonging to a dataset
    rec_id_2 = ds.get_record("test_molecule_2", "spec_1").id
    mol_id_2 = ds.get_entry("test_molecule_2").molecule.id

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id)
    assert query_res._current_meta.n_found == 2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id, molecule_id=mol_id_2)
    assert query_res._current_meta.n_found == 1
    assert list(query_res)[0].id == rec_id_2

    query_res = snowflake_client.query_singlepoints(dataset_id=ds.id + 1, molecule_id=mol_id_2)
    assert query_res._current_meta.n_found == 0

    # Query which dataset contains a record
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 1
    assert rec_info[0]["dataset_id"] == 1
    assert rec_info[0]["entry_name"] == "test_molecule_2"

    # Query which dataset contains a record
    ds.remove_records(entry_names="test_molecule_2", specification_names="spec_1", delete_records=True)
    rec_info = snowflake_client.query_dataset_records([rec_id_2])
    assert len(rec_info) == 0
