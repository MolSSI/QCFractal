from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcportal import PortalRequestError
from qcportal.datasets.singlepoint.models import SinglepointDataset, SinglepointDatasetNewEntry
from qcportal.molecules import Molecule
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.singlepoint.models import QCSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient

test_entries = [
    SinglepointDatasetNewEntry(molecule=Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, 2]), name="hydrogen_2"),
    SinglepointDatasetNewEntry(
        molecule=Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, 4]),
        name="HYDrogen_4",
        comment="a comment",
        attributes={"internal": "h2"},
    ),
    SinglepointDatasetNewEntry(
        molecule=Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, 6]),
        name="hydrogen_6",
        additional_keywords={"maxiter": 1000},
    ),
]

test_specs = [
    QCSpecification(program="prog1", driver="energy", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}),
    QCSpecification(program="prog2", driver="energy", method="hf", basis="sto-3g", keywords={"maxiter": 40}),
]


def test_singlepoint_dataset_model_add_get_entry(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ent_map = {x.name: x for x in test_entries}

    meta = ds.add_entries(test_entries)
    assert meta.success
    assert meta.n_inserted == len(test_entries)

    assert set(ds.entry_names) == set(ent_map.keys())
    ents = list(ds.iterate_entries())
    assert set(x.name for x in ents) == set(ent_map.keys())

    # Now with a fresh dataset
    ds: SinglepointDataset = snowflake_client.get_dataset_by_id(ds.id)

    assert set(ds.entry_names) == set(ent_map.keys())

    ents = list(ds.iterate_entries(include=["molecule"]))
    assert set(x.name for x in ents) == set(ent_map.keys())

    # Another fresh dataset
    ds: SinglepointDataset = snowflake_client.get_dataset_by_id(ds.id)

    for ent in ent_map.values():
        test_ent = ds.get_entry(ent.name, include=["molecule"])
        assert test_ent.name == ent.name
        assert test_ent.comment == ent.comment
        assert test_ent.molecule == ent.molecule
        assert test_ent.attributes == ent.attributes
        assert test_ent.additional_keywords == ent.additional_keywords


def test_singlepoint_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ent1 = test_entries[0]

    # Same name, different molecule
    ent2 = test_entries[1].copy(update={"name": ent1.name})

    meta = ds.add_entries([ent1])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0

    meta = ds.add_entries([ent2])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 1

    assert ds.entry_names == ["hydrogen_2"]
    ents = list(ds.iterate_entries(include=["molecule"]))
    assert len(ents) == 1

    # Now with a fresh dataset
    ds: SinglepointDataset = snowflake_client.get_dataset_by_id(ds.id)

    assert ds.entry_names == ["hydrogen_2"]

    ents = list(ds.iterate_entries(include=["molecule"]))
    assert len(ents) == 1
    assert ents[0].molecule == test_entries[0].molecule  # the original molecule


def test_singlepoint_dataset_model_delete_entry(snowflake_client: PortalClient):

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries)
    ds.submit()

    ent_rec_map = {entry_name: record for entry_name, _, record in ds.iterate_records()}
    assert len(ent_rec_map) == 3

    meta = ds.delete_entries("hydrogen_2", False)
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_children_deleted == 0
    snowflake_client.get_records(ent_rec_map["hydrogen_2"].id)  # exception if it doesn't exist

    meta = ds.delete_entries("hydrogen_6", True)  # delete record, too
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_children_deleted == 1
    assert snowflake_client.get_records(ent_rec_map["hydrogen_6"].id, missing_ok=True) is None

    # Were removed from the model
    for name in ["hydrogen_2", "hydrogen_6"]:
        assert len(ds.raw_data.record_map) == 1
        assert name not in ds.raw_data.entries
        assert name not in ds.raw_data.entry_names
        assert all(name != x for x, _ in ds.raw_data.record_map.keys())
        assert all(name != ent_name for ent_name, _, _ in ds.iterate_records())

    # Delete when it doesn't exist
    meta = ds.delete_entries("hydrogen_6", True)
    assert meta.success is False
    assert meta.n_deleted == 0
    assert meta.n_children_deleted == 0
    assert meta.error_idx == [0]


def test_singlepoint_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    meta = ds.add_specification("spec_1", test_specs[0])
    assert meta.success
    assert meta.n_inserted == 1

    meta = ds.add_specification("spec_2", test_specs[1], description="a description")
    assert meta.success
    assert meta.n_inserted == 1

    assert set(ds.specifications.keys()) == {"spec_1", "spec_2"}

    # Now with a fresh dataset
    ds: SinglepointDataset = snowflake_client.get_dataset_by_id(ds.id)
    assert set(ds.specifications.keys()) == {"spec_1", "spec_2"}

    assert ds.specifications["spec_1"].name == "spec_1"
    assert ds.specifications["spec_1"].specification == test_specs[0]
    assert ds.specifications["spec_1"].description is None

    assert ds.specifications["spec_2"].name == "spec_2"
    assert ds.specifications["spec_2"].specification == test_specs[1]
    assert ds.specifications["spec_2"].description == "a description"


def test_singlepoint_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    meta = ds.add_specification("spec_1", test_specs[0])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0

    # Same name, different spec
    meta = ds.add_specification("spec_1", test_specs[1], description="a description")
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 1

    assert set(ds.specifications.keys()) == {"spec_1"}

    # Now with a fresh dataset
    ds: SinglepointDataset = snowflake_client.get_dataset_by_id(ds.id)
    assert set(ds.specifications.keys()) == {"spec_1"}

    assert ds.specifications["spec_1"].name == "spec_1"
    assert ds.specifications["spec_1"].specification == test_specs[0]
    assert ds.specifications["spec_1"].description is None


def test_singlepoint_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries[0])
    ds.submit()

    spec_rec_map = {spec_name: record for _, spec_name, record in ds.iterate_records()}
    assert len(spec_rec_map) == 2

    meta = ds.delete_specification("spec_1", False)
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_children_deleted == 0
    snowflake_client.get_records(spec_rec_map["spec_1"].id)  # exception if it doesn't exist

    meta = ds.delete_specification("spec_2", True)  # delete record, too
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_children_deleted == 1
    assert snowflake_client.get_records(spec_rec_map["spec_2"].id, missing_ok=True) is None

    # Were removed from the model
    for name in ["spec_1", "spec_2"]:
        assert len(ds.raw_data.record_map) == 0
        assert name not in ds.raw_data.specifications
        assert all(name != x for _, x in ds.raw_data.record_map.keys())
        assert all(name != spec_name for _, spec_name, _ in ds.iterate_records())

    # Delete when it doesn't exist
    meta = ds.delete_specification("spec_1", True)
    assert meta.success is False
    assert meta.n_deleted == 0
    assert meta.n_children_deleted == 0
    assert meta.error_idx == [0]


def test_singlepoint_dataset_model_remove_record(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries)
    ds.submit()

    all_records = list(ds.iterate_records())
    assert len(all_records) == 6

    rec_ids = [r.id for _, _, r in all_records]

    ds.remove_records(["hydrogen_2", "hydrogen_6"], ["spec_1"], delete_records=False)
    all_records_2 = list(ds.iterate_records())
    assert len(all_records_2) == 4

    # Record ids should exist in the db
    snowflake_client.get_records(rec_ids)

    # Now with a fresh dataset
    ds: SinglepointDataset = snowflake_client.get_dataset_by_id(ds.id)
    all_records_2 = list(ds.iterate_records())
    assert len(all_records_2) == 4

    # Delete records as well
    to_delete_id = ds.get_record("hydrogen_2", "spec_2").id
    ds.remove_records("hydrogen_2", "spec_2", delete_records=True)
    all_records_2 = list(ds.iterate_records())
    assert len(all_records_2) == 3

    # Should be one missing
    recs = snowflake_client.get_records(rec_ids, missing_ok=True)
    assert recs.count(None) == 1
    none_idx = recs.index(None)
    assert rec_ids[none_idx] == to_delete_id


def test_singlepoint_dataset_model_submit(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset(
        "singlepoint", "Test dataset", default_tag="default_tag", default_priority=PriorityEnum.low
    )

    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries[0])
    ds.submit()

    all_records = list(ds.iterate_records())
    assert len(all_records) == 1
    rec = all_records[0][2]
    assert rec.status == RecordStatusEnum.waiting
    assert rec.specification == test_specs[0]
    assert rec.molecule == test_entries[0].molecule

    # Used default tag/priority
    assert rec.task.tag == "default_tag"
    assert rec.task.priority == PriorityEnum.low

    # Now additional keywords
    ds.add_entries(test_entries[2])
    ds.submit()
    rec = ds.get_record(test_entries[2].name, "spec_1")
    assert rec.molecule == test_entries[2].molecule

    expected_kw = test_specs[0].keywords.copy()
    expected_kw.update(test_entries[2].additional_keywords)
    assert rec.specification.keywords == expected_kw

    # Additional submission stuff
    ds.add_entries(test_entries[1])
    ds.submit(tag="new_tag", priority=PriorityEnum.high)
    rec = ds.get_record(test_entries[1].name, "spec_1")
    assert rec.task.tag == "new_tag"
    assert rec.task.priority == PriorityEnum.high

    # But didn't change others
    rec = ds.get_record(test_entries[2].name, "spec_1")
    assert rec.task.tag == "default_tag"
    assert rec.task.priority == PriorityEnum.low


def test_singlepoint_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.submit()  # should be ok

    with pytest.raises(PortalRequestError, match="Could not find all entries"):
        ds.submit(entry_names="entry_1")
    with pytest.raises(PortalRequestError, match="Could not find all specifications"):
        ds.submit(specification_names="spec_1")


def test_singlepoint_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries)
    ds.submit()

    assert all(x.status == RecordStatusEnum.waiting for _, _, x in ds.iterate_records())

    time_0 = datetime.utcnow()
    ds.cancel_records("hydrogen_2", "spec_1")
    time_1 = datetime.utcnow()

    # Should be automatically updated when we iterate
    all_records = list(ds.iterate_records())
    cancelled = [(e, s, r) for e, s, r in all_records if r.status == RecordStatusEnum.cancelled]
    assert len(cancelled) == 1
    assert cancelled[0][0] == "hydrogen_2"
    assert cancelled[0][1] == "spec_1"
    assert time_0 < cancelled[0][2].modified_on < time_1

    # Do another one
    ds.cancel_records("hydrogen_6", "spec_1")

    # Disable auto updating
    all_records = list(ds.iterate_records(fetch_updated=False))
    cancelled = [(e, s, r) for e, s, r in all_records if r.status == RecordStatusEnum.cancelled]
    assert len(cancelled) == 1  # did not fetch the newly-cancelled one

    # Now force update
    all_records = list(ds.iterate_records(fetch_updated=False, force_refetch=True))
    cancelled = [(e, s, r) for e, s, r in all_records if r.status == RecordStatusEnum.cancelled]
    assert len(cancelled) == 2  # fetched them all


def test_singlepoint_dataset_model_modify_records(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries[0])
    ds.add_entries(test_entries[1])
    ds.submit()

    entry_name = test_entries[0].name
    entry_name_2 = test_entries[1].name
    spec_name = "spec_1"

    rec = ds.get_record(entry_name, spec_name)
    assert rec.status == RecordStatusEnum.waiting

    # All records
    ds.cancel_records()
    rec = ds.get_record(entry_name, spec_name)
    rec2 = ds.get_record(entry_name_2, spec_name)
    assert rec.status == RecordStatusEnum.cancelled
    assert rec2.status == RecordStatusEnum.cancelled

    ds.uncancel_records()
    rec = ds.get_record(entry_name, spec_name)
    rec2 = ds.get_record(entry_name_2, spec_name)
    assert rec.status == RecordStatusEnum.waiting
    assert rec2.status == RecordStatusEnum.waiting

    # Single record
    ds.cancel_records(entry_name, spec_name)
    rec = ds.get_record(entry_name, spec_name)
    rec2 = ds.get_record(entry_name_2, spec_name)
    assert rec.status == RecordStatusEnum.cancelled
    assert rec2.status == RecordStatusEnum.waiting

    ds.uncancel_records(entry_name, spec_name)
    rec = ds.get_record(entry_name, spec_name)
    rec2 = ds.get_record(entry_name_2, spec_name)
    assert rec.status == RecordStatusEnum.waiting
    assert rec2.status == RecordStatusEnum.waiting

    ds.modify_records(
        entry_name, spec_name, new_tag="new_tag", new_priority=PriorityEnum.low, new_comment="a new comment"
    )
    rec = ds.get_record(entry_name, spec_name)
    assert rec.status == RecordStatusEnum.waiting
    assert rec.task.tag == "new_tag"
    assert rec.task.priority == PriorityEnum.low
    assert rec.comments[0].comment == "a new comment"
