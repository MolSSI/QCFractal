from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting.helpers import test_users, read_record_data
from qcfractal.components.singlepoint.testing_helpers import load_procedure_data, run_procedure_data
from qcportal import PortalRequestError
from qcportal.compare_records import compare_records
from qcportal.record_models import PriorityEnum, RecordStatusEnum, record_from_dict
from qcportal.singlepoint import SinglepointInput

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
    assert plist[0]["tagline"] == proj.tagline
    assert plist[0]["tags"] == proj.tags
    assert plist[0]["owner_user"] == proj.owner_user


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


def test_project_client_link_records_datasets(snowflake_client: PortalClient):

    # Add these directly to the server (not part of the project)
    meta, rids = snowflake_client.add_singlepoints(
        molecules=[test_inp_1.molecule],
        program=test_inp_1.specification.program,
        driver=test_inp_1.specification.driver,
        method=test_inp_1.specification.method,
        basis=test_inp_1.specification.basis,
        keywords=test_inp_1.specification.keywords,
        compute_tag="test_compute_tag",
        compute_priority=PriorityEnum.low,
    )

    assert meta.success

    ds1 = snowflake_client.add_dataset(
        "singlepoint", "test singlepoint dataset", description="description", tagline="tagline", tags=["tag2"]
    )
    ds2 = snowflake_client.add_dataset(
        "optimization", "test optimization dataset", description="description 2", tagline="tagline 2", tags=["tag4"]
    )

    proj = snowflake_client.add_project(
        "test project",
        default_compute_tag="test_compute_tag",
        default_compute_priority=PriorityEnum.low,
    )

    linked_ds1 = proj.link_dataset(ds1.id)

    # Changing the name
    linked_ds2 = proj.link_dataset(
        ds2.id, name="new name", description="new description", tagline="new tagline", tags=["tag3"]
    )

    linked_r = proj.link_record(rids[0], "record_name", "description", tags=["tag1"])

    # Refetch to make sure it's changed on the server
    linked_ds1 = proj.get_dataset("test singlepoint dataset")
    linked_ds2 = proj.get_dataset("new name")
    linked_r = proj.get_record("record_name")

    assert linked_r.id == rids[0]
    assert linked_r.name == "record_name"
    assert linked_r.description == "description"
    assert linked_r.tags == ["tag1"]

    assert linked_ds1.id == ds1.id
    assert linked_ds1.name == "test singlepoint dataset"
    assert linked_ds1.description == "description"
    assert linked_ds1.tagline == "tagline"
    assert linked_ds1.tags == ["tag2"]

    assert linked_ds2.id == ds2.id
    assert linked_ds2.name == "new name"
    assert linked_ds2.description == "new description"
    assert linked_ds2.tagline == "new tagline"
    assert linked_ds2.tags == ["tag3"]

    # Can't link again
    with pytest.raises(PortalRequestError, match="Dataset.*already linked"):
        proj.link_dataset(ds1.id)

    with pytest.raises(PortalRequestError, match="Record.*already linked"):
        proj.link_record(rids[0], "record_name", "description", tags=["tag1"])

    # Unlink stuff
    proj.unlink_datasets("test singlepoint dataset")
    proj.unlink_records("record_name")
    proj.unlink_datasets("new name")

    assert len(proj.dataset_metadata) == 0
    assert len(proj.record_metadata) == 0
    proj.fetch_dataset_metadata()
    proj.fetch_record_metadata()
    assert len(proj.dataset_metadata) == 0
    assert len(proj.record_metadata) == 0


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

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")
    run_procedure_data(storage_socket, activated_manager_name, "sp_psi4_peroxide_energy_wfn")

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


def test_project_client_import_records(secure_snowflake: QCATestingSnowflake):

    client = secure_snowflake.client("submit_user", test_users["submit_user"]["pw"])
    proj = client.add_project("test project")

    test_files = [
        "go_error_17761737",
        "go_H2O2_psi4_b3lyp",
        "mb_all_he4_psi4_multiss",
        "neb_error_68251163",
        "neb_HCN_psi4_b3lyp_opt3",
        "opt_error_118868739",
        "opt_psi4_benzene",
        "rxn_error_118326390",
        "rxn_H2O_psi4_mp2_optsp",
        "sp_error_119608646",
        "sp_psi4_benzene_energy_1",
        "sp_psi4_h2_b3lyp_nativefiles",
        "sp_psi4_peroxide_energy_wfn",
        "td_error_137174663",
        "td_H2O2_mopac_pm6",
    ]

    test_data = {}
    id_map = {}

    for t_name in test_files:
        d = read_record_data(t_name)
        r = record_from_dict(d)
        test_data[t_name] = r

        tag = t_name.split("_")[0]
        server_r = proj.import_record(t_name, r, description=f"Record {t_name}", tags=["imported", tag])
        id_map[t_name] = server_r.id

    # Check that they are there
    proj = client.get_project("test project")
    for rm in proj.record_metadata:
        assert rm.tags == ["imported", rm.name.split("_")[0]]
        server_r = proj.get_record(rm.record_id, include=["**"])
        server_r.fetch_children(include=["**"])

        test_r = test_data[rm.name]
        compare_records(server_r, test_r)
        assert server_r.creator_user == "submit_user"
