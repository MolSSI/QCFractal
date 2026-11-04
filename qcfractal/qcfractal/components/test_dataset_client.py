from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.dataset_db_models import BaseDatasetORM
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import load_procedure_data, run_procedure_data
from qcportal import PortalRequestError
from qcportal.exceptions import MissingDataError
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum, RecordStatusEnum
from qcportal.dataset_models import (
    DatasetFetchPropertiesBody,
    DatasetFetchPropertiesResult,
    MAX_FETCH_PROPERTIES,
    create_dataset_view,
    load_dataset_view,
)
from qcportal.serialization import deserialize
from qcportal.singlepoint import SinglepointDataset, QCSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcarchivetesting.testing_classes import QCATestingSnowflake


@pytest.mark.parametrize(
    "dataset_type", ["singlepoint", "optimization", "torsiondrive", "gridoptimization", "manybody", "reaction", "neb"]
)
def test_dataset_client_add_get(submitter_client: PortalClient, dataset_type: str):
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
    assert ds.creator_user == submitter_client.username

    # creator_user is not (yet) part of the wire format
    r = submitter_client._request("get", f"api/v1/datasets/{ds.id}")
    raw_ds = deserialize(r.content, r.headers["Content-Type"], dict)
    assert "creator_user" not in raw_ds

    # The client model fills creator_user in from owner_user
    assert ds.creator_user == submitter_client.username

    # List summaries have always included creator_user
    ds_list = submitter_client.list_datasets()
    assert ds_list[0]["owner_user"] == submitter_client.username
    assert ds_list[0]["creator_user"] == submitter_client.username

    # case insensitive
    ds2 = submitter_client.get_dataset(dataset_type, "test DATASET")
    assert ds2.id == ds.id


def test_dataset_client_add_default_compute_tag_priority(submitter_client: PortalClient):
    ds = submitter_client.add_dataset(
        "singlepoint",
        "Test dataset",
        default_compute_tag="new_tag",
        default_compute_priority=PriorityEnum.high,
    )

    assert ds.default_compute_tag == "new_tag"
    assert ds.default_compute_priority == PriorityEnum.high


def test_dataset_client_add_default_tag_priority_deprecated(submitter_client: PortalClient):
    # 'default_tag'/'default_priority' are deprecated, but should still work
    ds = submitter_client.add_dataset(
        "singlepoint",
        "Test dataset",
        default_tag="old_tag",
        default_priority=PriorityEnum.low,
    )

    assert ds.default_compute_tag == "old_tag"
    assert ds.default_compute_priority == PriorityEnum.low


def test_dataset_client_status(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status() == {}

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")
    run_procedure_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)
    ds.submit()

    stat1 = ds.status()
    stat2 = snowflake_client.get_dataset_status_by_id(ds.id)
    assert stat1 == stat2


def test_dataset_client_status_by_tag(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.status_by_compute_tag() == []

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")

    molecule_2 = Molecule(symbols=["b"], geometry=[0, 0, 0])

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.add_entry(name="test_molecule_2", molecule=molecule_2)
    ds.submit(entry_names=["test_molecule"], compute_tag="tag1")
    ds.submit(entry_names=["test_molecule_2"], compute_tag="tag2")

    status = ds.status_by_compute_tag()
    assert len(status) == 2
    assert {s[0] for s in status} == {"tag1", "tag2"}
    assert all(s[1] == "waiting" for s in status)
    assert all(s[2] == 1 for s in status)


def test_dataset_client_reset_records_default_skips_running(snowflake: QCATestingSnowflake):
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()
    manager_name, _ = snowflake.activate_manager()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")

    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.submit()

    rec = ds.get_record("test_molecule", "spec_1")
    assert rec.status == RecordStatusEnum.waiting

    tasks = storage_socket.tasks.claim_tasks(
        manager_name.fullname, snowflake.activated_manager_programs(), ["*"], limit=1
    )
    assert len(tasks) == 1

    rec = ds.get_record("test_molecule", "spec_1", force_refetch=True)
    assert rec.status == RecordStatusEnum.running

    ds.reset_records(refetch_records=True)

    rec = ds.get_record("test_molecule", "spec_1", force_refetch=True)
    assert rec.status == RecordStatusEnum.running


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

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")
    run_procedure_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

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

    # Filter by dataset type (single string or list)
    rec_info = snowflake_client.query_dataset_records([rec_id_2], dataset_type="singlepoint")
    assert len(rec_info) == 1
    rec_info = snowflake_client.query_dataset_records([rec_id_2], dataset_type=["optimization", "singlepoint"])
    assert len(rec_info) == 1
    rec_info = snowflake_client.query_dataset_records([rec_id_2], dataset_type="optimization")
    assert len(rec_info) == 0

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

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")
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

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")
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

    input_spec, molecule, _ = load_procedure_data("sp_psi4_peroxide_energy_wfn")
    record_id = run_procedure_data(storage_socket, manager_name, "sp_psi4_peroxide_energy_wfn")

    # Add this as a part of the dataset
    ds.add_specification("spec_1", input_spec)
    ds.add_entry(name="test_molecule", molecule=molecule)
    ds.submit()

    assert ds.status()["spec_1"]["complete"] == 1

    computed_prop = ds.computed_properties
    assert "spec_1" in computed_prop
    assert "scf_total_energy" in computed_prop["spec_1"]
    assert "calcinfo_natom" in computed_prop["spec_1"]


def _setup_properties_dataset(snowflake: QCATestingSnowflake) -> SinglepointDataset:
    # A dataset whose records have known statuses and properties. Entry names are added out of
    # order to check that rows come back sorted by (entry name, specification name)
    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds.add_specification("spec_b", QCSpecification(program="prog", driver="energy", method="hf", basis="sto-3g"))
    ds.add_specification("spec_a", QCSpecification(program="prog", driver="energy", method="hf", basis="6-31g"))
    for i, name in enumerate(["c_mol", "a_mol", "d_mol", "b_mol"]):
        ds.add_entry(name=name, molecule=Molecule(symbols=["he"], geometry=[0, 0, i]))
    ds.submit()

    # status and properties of each record (missing combinations are left waiting)
    record_data = {
        ("a_mol", "spec_a"): (RecordStatusEnum.complete, {"energy": -1.5, "natom": 1, "vec": [1, 2], "entry": "x"}),
        ("a_mol", "spec_b"): (RecordStatusEnum.complete, {"energy": -2.5, "natom": 1}),
        ("b_mol", "spec_a"): (RecordStatusEnum.complete, None),
        ("b_mol", "spec_b"): (RecordStatusEnum.waiting, {"energy": 99.0}),
        ("c_mol", "spec_a"): (RecordStatusEnum.complete, {"energy": -3.0, "natom": 2}),
        ("c_mol", "spec_b"): (RecordStatusEnum.complete, {"energy": -4.0, "natom": 2}),
        ("d_mol", "spec_a"): (RecordStatusEnum.complete, {"energy": 4.0}),
        ("d_mol", "spec_b"): (RecordStatusEnum.error, {"energy": 98.0}),
    }

    record_ids = {(e, s): r.id for e, s, r in ds.iterate_records()}
    with storage_socket.session_scope() as session:
        for key, (status, properties) in record_data.items():
            record = session.get(BaseRecordORM, record_ids[key])
            record.status = status
            record.properties = properties

    return snowflake_client.get_dataset_by_id(ds.id)


def test_dataset_client_iterate_properties(snowflake: QCATestingSnowflake):
    ds = _setup_properties_dataset(snowflake)

    rows = list(ds.iterate_properties(["energy", "natom", "vec", "missing"]))

    # Only complete records, sorted by entry then specification
    assert [(e, s) for e, s, _, _ in rows] == [
        ("a_mol", "spec_a"),
        ("a_mol", "spec_b"),
        ("b_mol", "spec_a"),
        ("c_mol", "spec_a"),
        ("c_mol", "spec_b"),
        ("d_mol", "spec_a"),
    ]

    values = {(e, s): v for e, s, _, v in rows}
    assert values[("a_mol", "spec_a")] == [-1.5, 1, [1, 2], None]
    assert values[("b_mol", "spec_a")] == [None, None, None, None]
    assert values[("d_mol", "spec_a")] == [4.0, None, None, None]

    record_ids = {(e, s): r.id for e, s, r in ds.iterate_records()}
    assert all(record_ids[(e, s)] == rid for e, s, rid, _ in rows)

    # The same property twice gives two values
    assert next(iter(ds.iterate_properties(["natom", "natom"])))[3] == [1, 1]

    # Filters
    rows = list(ds.iterate_properties("energy", entry_names=["c_mol", "b_mol"], specification_names="spec_b"))
    assert rows == [("c_mol", "spec_b", record_ids[("c_mol", "spec_b")], [-4.0])]

    # Unknown names are an error
    with pytest.raises(MissingDataError, match="does_not_exist"):
        list(ds.iterate_properties("energy", entry_names=["does_not_exist"]))
    with pytest.raises(MissingDataError, match="does_not_exist"):
        list(ds.iterate_properties("energy", specification_names=["does_not_exist"], fetch_records=True))

    # Fetching whole records gives the same rows (in no particular order)
    all_properties = ["energy", "natom", "vec", "missing"]
    server_rows = list(ds.iterate_properties(all_properties))
    assert sorted(ds.iterate_properties(all_properties, fetch_records=True)) == server_rows

    # So does requesting more properties than the endpoint allows (which uses the records)
    many_properties = all_properties + [f"prop_{i}" for i in range(MAX_FETCH_PROPERTIES)]
    many_rows = sorted(ds.iterate_properties(many_properties))
    assert [(e, s, r, v[:4]) for e, s, r, v in many_rows] == server_rows

    # Duplicate entry names are only returned once
    rows = list(ds.iterate_properties("energy", entry_names=["c_mol", "c_mol"]))
    assert [(e, s) for e, s, _, _ in rows] == [("c_mol", "spec_a"), ("c_mol", "spec_b")]


def test_dataset_client_fetch_properties_paginate(snowflake: QCATestingSnowflake):
    ds = _setup_properties_dataset(snowflake)
    snowflake_client = snowflake.client()

    def fetch(**kwargs) -> DatasetFetchPropertiesResult:
        body = DatasetFetchPropertiesBody(properties=["energy"], **kwargs)
        return snowflake_client.make_request(
            "post", f"api/v1/datasets/singlepoint/{ds.id}/properties/bulkFetch", DatasetFetchPropertiesResult, body=body
        )

    pages = []
    cursor = None
    while True:
        result = fetch(cursor=cursor, limit=4, include_total=cursor is None)
        pages.append(result)
        cursor = result.next_cursor
        if cursor is None:
            break

    assert [len(p.rows) for p in pages] == [4, 2]
    assert pages[0].total == 6
    assert pages[1].total is None
    assert pages[0].next_cursor == ("c_mol", "spec_a")

    all_rows = [r for p in pages for r in p.rows]
    assert all_rows == fetch().rows

    # Exactly filling a page does not leave an empty page behind
    result = fetch(limit=6)
    assert len(result.rows) == 6
    assert result.next_cursor is None

    # The total respects the filters
    assert fetch(specification_names=["spec_b"], include_total=True).total == 2

    # The limit is capped by the server
    api_limit = snowflake_client.api_limits["get_records"]
    assert len(fetch(limit=api_limit + 10).rows) == 6

    # Invalid limits, and too many properties (sent as raw bodies, since the
    # request model would reject them before they reach the server)
    url = f"api/v1/datasets/singlepoint/{ds.id}/properties/bulkFetch"
    invalid_bodies = [
        {"properties": ["energy"], "limit": 0},
        {"properties": ["energy"], "limit": -1},
        {"properties": [f"prop_{i}" for i in range(MAX_FETCH_PROPERTIES + 1)]},
    ]
    for body in invalid_bodies:
        with pytest.raises(PortalRequestError, match="Invalid body"):
            snowflake_client.make_request("post", url, DatasetFetchPropertiesResult, body=body)


def test_dataset_client_get_properties_df(snowflake: QCATestingSnowflake):
    import pandas as pd

    ds = _setup_properties_dataset(snowflake)
    snowflake_client = snowflake.client()

    df = ds.get_properties_df(["energy", "natom", "missing"])
    df_records = ds.get_properties_df(["energy", "natom", "missing"], fetch_records=True)
    pd.testing.assert_frame_equal(df, df_records)

    # All-missing columns are dropped, and an entry with no non-missing values is still a row
    assert set(df.columns) == {("spec_a", "energy"), ("spec_a", "natom"), ("spec_b", "energy"), ("spec_b", "natom")}
    assert list(df.index) == ["a_mol", "b_mol", "c_mol", "d_mol"]
    assert df.loc["c_mol", ("spec_b", "energy")] == -4.0

    df = ds.get_properties_df(["energy"], entry_names=["a_mol", "c_mol"], specification_names=["spec_a"])
    df_records = ds.get_properties_df(
        ["energy"], entry_names=["a_mol", "c_mol"], specification_names=["spec_a"], fetch_records=True
    )
    pd.testing.assert_frame_equal(df, df_records)

    # A property with the same name as one of the index columns, with each way of fetching
    many_properties = ["entry", "energy"] + [f"prop_{i}" for i in range(MAX_FETCH_PROPERTIES)]
    for kwargs in ({}, {"fetch_records": True}):
        df = ds.get_properties_df(["entry", "energy"], **kwargs)
        assert df.loc["a_mol", ("spec_a", "entry")] == "x"
        assert ("spec_b", "entry") not in df.columns

    # Too many properties for the endpoint falls back to fetching records
    df = ds.get_properties_df(many_properties)
    pd.testing.assert_frame_equal(df, ds.get_properties_df(["entry", "energy"], fetch_records=True))

    # No matching records gives the same (empty) DataFrame either way (b_mol/spec_b is not complete)
    df = ds.get_properties_df(["energy"], entry_names=["b_mol"], specification_names=["spec_b"])
    df_records = ds.get_properties_df(
        ["energy"], entry_names=["b_mol"], specification_names=["spec_b"], fetch_records=True
    )
    assert df.empty
    pd.testing.assert_frame_equal(df, df_records)

    with pytest.raises(ValueError, match="duplicate"):
        ds.get_properties_df(["energy", "energy"])
    with pytest.raises(ValueError, match="at least one"):
        ds.get_properties_df([])

    # Duplicate entry/specification names are only counted once
    for kwargs in ({}, {"fetch_records": True}):
        df = ds.get_properties_df(
            ["energy"], entry_names=["c_mol", "a_mol", "c_mol"], specification_names=["spec_a", "spec_a"], **kwargs
        )
        assert list(df.index) == ["a_mol", "c_mol"]
        assert list(df.columns) == [("spec_a", "energy")]

    # compile_values with no matching records gives an empty DataFrame of the usual shape
    empty = ds.compile_values(
        lambda r: [r.properties["energy"]],
        value_names=["energy"],
        unpack=True,
        entry_names=["b_mol"],
        specification_names=["spec_b"],
    )
    nonempty = ds.compile_values(
        lambda r: [r.properties["energy"]], value_names=["energy"], unpack=True, entry_names=["a_mol"]
    )
    assert empty.empty
    assert empty.index.name == nonempty.index.name == "entry"
    assert empty.columns.names == nonempty.columns.names

    # Names not in the dataset are an error either way
    for kwargs in ({}, {"fetch_records": True}):
        with pytest.raises(MissingDataError, match="entry_names contains names not in this dataset: typo"):
            ds.get_properties_df(["energy"], entry_names=["a_mol", "typo"], **kwargs)
        with pytest.raises(MissingDataError, match="specification_names contains names not in this dataset: typo"):
            ds.get_properties_df(["energy"], specification_names=["typo"], **kwargs)

    # Names added since the dataset was loaded are found
    ds_new = snowflake_client.get_dataset_by_id(ds.id)
    ds_new.entry_names  # caches the current entry names
    ds.add_entry(name="e_mol", molecule=Molecule(symbols=["he"], geometry=[0, 0, 10]))
    assert ds_new.get_properties_df(["energy"], entry_names=["e_mol"]).empty


def test_dataset_client_get_properties_df_view(snowflake: QCATestingSnowflake, tmp_path):
    import pandas as pd

    ds = _setup_properties_dataset(snowflake)
    snowflake_client = snowflake.client()

    view_path = str(tmp_path / "view.sqlite")
    create_dataset_view(snowflake_client, ds.id, view_path)

    expected = ds.get_properties_df(["energy", "natom", "entry"])

    # A view loaded from a file (no server connection) uses the records in the file
    dsv = load_dataset_view(view_path)
    assert dsv.is_view and dsv.offline
    assert sorted(dsv.iterate_properties(["energy", "natom"])) == list(ds.iterate_properties(["energy", "natom"]))
    pd.testing.assert_frame_equal(dsv.get_properties_df(["energy", "natom", "entry"]), expected)

    df = dsv.get_properties_df(["energy"], entry_names=["a_mol"], specification_names=["spec_b"])
    assert df.loc["a_mol", ("spec_b", "energy")] == -2.5

    with pytest.raises(MissingDataError, match="typo"):
        dsv.get_properties_df(["energy"], entry_names=["typo"])

    # An online dataset using a view as its cache
    ds_cached = snowflake_client.get_dataset_by_id(ds.id)
    ds_cached.use_view_cache(view_path)
    pd.testing.assert_frame_equal(
        ds_cached.get_properties_df(["energy", "natom", "entry"], fetch_records=True), expected
    )


def test_dataset_client_contributed_values_empty(snowflake_client: PortalClient):
    # contributed_values are legacy data with no API for adding them - only datasets migrated
    # from an old QCFractal version have any. A dataset with none must not raise on access; the
    # server previously sent an empty *list*, which failed validation against the client's
    # dict-shaped response model.
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    assert ds.contributed_values == {}


def test_dataset_client_contributed_values(snowflake: QCATestingSnowflake):
    from qcfractal.components.dataset_db_models import ContributedValuesORM

    snowflake_client = snowflake.client()
    storage_socket = snowflake.get_storage_socket()

    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")

    # No API exists for adding contributed values - they only ever arrive via a migration
    # from an old QCFractal version, so insert directly into the database to simulate that
    with storage_socket.session_scope() as session:
        cv = ContributedValuesORM(
            dataset_id=ds.id,
            name="cv1",
            values=[1.0, 2.0, 3.0],
            index=["mol1", "mol2", "mol3"],
            values_structure={},
            theory_level="B3LYP/6-31G",
            units="hartree",
        )
        session.add(cv)
        session.commit()

    cvals = ds.contributed_values
    assert set(cvals.keys()) == {"cv1"}
    assert cvals["cv1"].name == "cv1"
    assert cvals["cv1"].values == [1.0, 2.0, 3.0]
    assert cvals["cv1"].theory_level == "B3LYP/6-31G"
    assert cvals["cv1"].units == "hartree"


def test_dataset_client_copy_from_incompatible(snowflake_client: PortalClient):
    ds_1 = snowflake_client.add_dataset("singlepoint", "Test sp dataset")
    ds_2 = snowflake_client.add_dataset("optimization", "Test opt dataset")

    with pytest.raises(PortalRequestError, match="does not match destination type"):
        ds_2.copy_records_from(ds_1.id)


def test_dataset_client_clone_owner(secure_snowflake: QCATestingSnowflake):
    submit_client = secure_snowflake.user_client("submit_user")
    admin_client = secure_snowflake.user_client("admin_user")

    ds = submit_client.add_dataset("singlepoint", "Test dataset")
    ds_clone = admin_client.clone_dataset(ds.id, "Cloned dataset")

    # The user doing the cloning owns (and created) the clone
    assert ds_clone.owner_user == "admin_user"
    assert ds_clone.creator_user == "admin_user"

    storage_socket = secure_snowflake.get_storage_socket()
    with storage_socket.session_scope() as session:
        clone_orm = session.get(BaseDatasetORM, ds_clone.id)
        assert clone_orm.owner_user.username == "admin_user"
        assert clone_orm.creator_user.username == "admin_user"
