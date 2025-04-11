from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import pytest

from qcportal import PortalRequestError
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.singlepoint import SinglepointInput
from qcfractal.components.singlepoint.testing_helpers import load_test_data as load_test_data, run_test_data

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcarchivetesting.testing_classes import QCATestingSnowflake


test_inp_1 = SinglepointInput(
    molecule={"symbols": ["h", "h"], "geometry": [0, 0, 0, 0, 0, 2]},
    specification={
        "program": "psi4",
        "driver": "energy",
        "method": "hf",
        "basis": "sto-3g",
    },
)


def test_project_client_add_get(submitter_client: PortalClient):
    proj = submitter_client.add_project(
        "test project",
        "Test Description",
        "a Tagline",
        ["tag1", "tag2"],
        "test_compute_tag",
        PriorityEnum.low,
        {"meta_key_1": "meta_value_1"},
    )

    assert proj.name == "test project"
    assert proj.description == "Test Description"
    assert proj.tagline == "a Tagline"
    assert proj.tags == ["tag1", "tag2"]
    assert proj.default_compute_tag == "test_compute_tag"
    assert proj.default_compute_priority == PriorityEnum.low
    assert proj.extras == {"meta_key_1": "meta_value_1"}

    assert proj.owner_user == submitter_client.username

    # case insensitive
    proj2 = submitter_client.get_project("TEST PrOJECT")
    assert proj2.id == proj.id

    plist = submitter_client.list_projects()
    assert plist[0]["id"] == proj.id
    assert plist[0]["project_name"] == proj.name


def test_project_client_add_get_records_datasets(snowflake_client: PortalClient):
    proj = snowflake_client.add_project(
        "test project",
        default_compute_tag="test_compute_tag",
        default_compute_priority=PriorityEnum.low,
    )

    r = proj.add_record("test_record", test_inp_1)
    ds1 = proj.add_dataset("singlepoint", "test singlepoint dataset")
    ds2 = proj.add_dataset("optimization", "test optimization dataset")

    r_test = proj.get_record("test_record")
    ds1_test = proj.get_dataset("test singlepoint dataset")
    ds2_test = proj.get_dataset("test optimization dataset")

    assert r_test.id == r.id
    assert r.task.compute_tag == "test_compute_tag"
    assert r.task.compute_priority == PriorityEnum.low

    assert ds1_test.id == ds1.id
    assert ds1.default_compute_tag == "test_compute_tag"
    assert ds1_test.default_compute_priority == PriorityEnum.low

    assert ds2_test.id == ds2.id
    assert ds2_test.default_compute_tag == "test_compute_tag"
    assert ds2_test.default_compute_priority == PriorityEnum.low

    plist = snowflake_client.list_projects()
    assert plist[0]["id"] == proj.id
    assert plist[0]["project_name"] == proj.name
    assert plist[0]["record_count"] == 1
    assert plist[0]["dataset_count"] == 2


def test_project_client_delete(snowflake_client: PortalClient):
    proj = snowflake_client.add_project("test project")

    ds1 = proj.add_dataset("singlepoint", "test singlepoint dataset")
    ds2 = proj.add_dataset("optimization", "test optimization dataset")

    r = proj.add_record("test_record", test_inp_1)

    snowflake_client.delete_project(proj.id)

    with pytest.raises(PortalRequestError, match="Could not find project"):
        snowflake_client.get_project("test project")

    with pytest.raises(PortalRequestError, match="Could not find project"):
        snowflake_client.get_project_by_id(proj.id)

    r_test = snowflake_client.get_records(r.id)
    ds1_test = snowflake_client.get_dataset_by_id(ds1.id)
    ds2_test = snowflake_client.get_dataset_by_id(ds2.id)

    assert r_test.id == r.id
    assert ds1_test.id == ds1.id and ds1_test.name == "test singlepoint dataset"
    assert ds2_test.id == ds2.id and ds2_test.name == "test optimization dataset"


def test_project_client_delete_with_records(snowflake_client: PortalClient):
    proj = snowflake_client.add_project("test project")

    ds1 = proj.add_dataset("singlepoint", "test singlepoint dataset")
    ds2 = proj.add_dataset("optimization", "test optimization dataset")
    r = proj.add_record("test_record", test_inp_1)

    snowflake_client.delete_project(proj.id, delete_records=True, delete_datasets=False)

    with pytest.raises(PortalRequestError, match="Could not find project"):
        snowflake_client.get_project("test project")

    r_test = snowflake_client.get_records(r.id, missing_ok=True)
    ds1_test = snowflake_client.get_dataset_by_id(ds1.id)
    ds2_test = snowflake_client.get_dataset_by_id(ds2.id)

    assert r_test is None
    assert ds1_test.id == ds1.id and ds1_test.name == "test singlepoint dataset"
    assert ds2_test.id == ds2.id and ds2_test.name == "test optimization dataset"


def test_project_client_delete_with_datasets(snowflake_client: PortalClient):
    proj = snowflake_client.add_project("test project")

    ds1 = proj.add_dataset("singlepoint", "test singlepoint dataset")
    ds2 = proj.add_dataset("optimization", "test optimization dataset")
    r = proj.add_record("test_record", test_inp_1)

    snowflake_client.delete_project(proj.id, delete_records=False, delete_datasets=True)

    with pytest.raises(PortalRequestError, match="Could not find project"):
        snowflake_client.get_project_by_id(proj.id)

    r_test = snowflake_client.get_records(r.id)
    assert r_test.id == r.id

    with pytest.raises(PortalRequestError, match="Could not find dataset"):
        snowflake_client.get_dataset_by_id(ds1.id)
    with pytest.raises(PortalRequestError, match="Could not find dataset"):
        snowflake_client.get_dataset_by_id(ds2.id)


def test_project_client_status(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    activated_manager_name, _ = snowflake.activate_manager()

    proj = snowflake_client.add_project(
        "test project",
        default_compute_tag="test_compute_tag",
        default_compute_priority=PriorityEnum.low,
    )

    ds = proj.add_dataset("singlepoint", "test singlepoint dataset")

    input_spec, molecule, _ = load_test_data("sp_psi4_peroxide_energy_wfn")
    run_test_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule, additional_keywords={"maxiter": 999})
    ds.submit()

    # Also add a lone record
    r = proj.add_record("test_record", test_inp_1)

    status = proj.status()
    assert len(status["records"]) == 1
    assert status["records"][RecordStatusEnum.waiting] == 1
    assert status["datasets"] == {RecordStatusEnum.complete: 1, RecordStatusEnum.waiting: 1}


def test_project_client_add_duplicates(snowflake_client: PortalClient):
    proj = snowflake_client.add_project("test project")

    proj.add_record("test_record", test_inp_1)
    proj.add_dataset("singlepoint", "test singlepoint dataset")
    proj.add_dataset("optimization", "test optimization dataset")

    with pytest.raises(PortalRequestError, match="Record 'test_record' already exists in project"):
        proj.add_record("test_record", test_inp_1)

    with pytest.raises(PortalRequestError, match="Dataset 'test optimization dataset' already exists in project"):
        proj.add_dataset("optimization", "test optimization dataset")

    with pytest.raises(PortalRequestError, match="Dataset 'test optimization dataset' already exists in project"):
        proj.add_dataset("optimization", "test optimization dataset")
