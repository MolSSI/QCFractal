from __future__ import annotations

import pytest

from qcarchivetesting.helpers import test_users
from qcportal import PortalRequestError
from qcportal.metadata_models import InsertMetadata, InsertCountsMetadata
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.utils import now_at_utc


@pytest.fixture(scope="function")
def dataset_submit_test_client(secure_snowflake):
    secure_snowflake.start_job_runner()
    client = secure_snowflake.client("submit_user", test_users["submit_user"]["pw"])
    yield client


def _compare_entries(ent1, ent2, entry_extra_compare):
    assert ent1.name == ent2.name
    assert ent1.comment == ent2.comment
    assert ent1.attributes == ent2.attributes
    entry_extra_compare(ent1, ent2)


def run_dataset_model_add_get_entry(snowflake_client, ds, test_entries, entry_extra_compare, background):
    ent_map = {x.name: x for x in test_entries}

    if background:
        ij = ds.background_add_entries(test_entries)
        ij.watch(interval=0.1, timeout=10)
        meta = InsertMetadata(**ij.result)
        ds.fetch_entries()
    else:
        meta = ds.add_entries(test_entries)

    assert meta.success
    assert meta.n_inserted == len(test_entries)

    assert set(ds.entry_names) == set(ent_map.keys())
    assert set(ds._entry_names) == set(ent_map.keys())
    assert set(ds.entry_names) == set(ds._cache_data.get_entry_names())
    ents = list(ds.iterate_entries())
    assert set(x.name for x in ents) == set(ent_map.keys())

    # Now with a fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)

    assert set(ds.entry_names) == set(ent_map.keys())
    ents = list(ds.iterate_entries())
    assert set(ds.entry_names) == set(ds._cache_data.get_entry_names())
    assert set(x.name for x in ents) == set(ent_map.keys())

    # Another fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)

    for ent in ent_map.values():
        test_ent = ds.get_entry(ent.name)
        _compare_entries(test_ent, ent, entry_extra_compare)

    # Get directly from the cache
    for ent in ent_map.values():
        test_ent = ds._cache_data.get_entry(ent.name)
        _compare_entries(test_ent, ent, entry_extra_compare)


def run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare):
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

    assert ds.entry_names == [test_entries[0].name]
    ents = list(ds.iterate_entries())
    assert len(ents) == 1
    assert ds._cache_data.get_entry_names() == [test_entries[0].name]

    # Now with a fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)

    assert ds.entry_names == [test_entries[0].name]

    ents = list(ds.iterate_entries())
    assert len(ents) == 1
    assert ds._cache_data.get_entry_names() == [test_entries[0].name]

    # Should be the molecule from the first entry
    entry_extra_compare(ents[0], test_entries[0])


def run_dataset_model_rename_entry(snowflake_client, ds, test_entries, test_specs):
    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries)
    ds.submit()
    ds.fetch_records()

    ent_rec_map = {entry_name: record for entry_name, _, record in ds.iterate_records()}
    assert len(ent_rec_map) == 3

    entry_name_1 = test_entries[0].name
    entry_name_2 = test_entries[1].name
    entry_name_3 = test_entries[2].name

    ds.rename_entries({entry_name_1: entry_name_1 + "new", entry_name_2: entry_name_2 + "new"})

    assert set(ds.entry_names) == {entry_name_1 + "new", entry_name_2 + "new", entry_name_3}
    assert set(ds._entry_names) == {entry_name_1 + "new", entry_name_2 + "new", entry_name_3}
    assert set(ds._cache_data.get_entry_names()) == {entry_name_1 + "new", entry_name_2 + "new", entry_name_3}

    with pytest.raises(PortalRequestError, match=r"Missing 1 entries"):
        ds.get_record(entry_name_1, "spec_1")
    with pytest.raises(PortalRequestError, match=r"Missing 1 entries"):
        ds.get_record(entry_name_2, "spec_1")

    assert ds.get_record(entry_name_3, "spec_1").id == ent_rec_map[entry_name_3].id
    assert ds._cache_data.get_entry(entry_name_1 + "new").name == entry_name_1 + "new"
    assert ds._cache_data.get_entry(entry_name_2 + "new").name == entry_name_2 + "new"

    assert ds._cache_data.get_dataset_record(entry_name_1, "spec_1") is None
    assert ds._cache_data.get_dataset_record(entry_name_2, "spec_1") is None

    assert ds._cache_data.get_dataset_record(entry_name_1 + "new", "spec_1").id == ent_rec_map[entry_name_1].id
    assert ds._cache_data.get_dataset_record(entry_name_2 + "new", "spec_1").id == ent_rec_map[entry_name_2].id
    assert ds._cache_data.get_dataset_record(entry_name_3, "spec_1").id == ent_rec_map[entry_name_3].id

    # Now with a fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert set(ds.entry_names) == {entry_name_1 + "new", entry_name_2 + "new", entry_name_3}
    ents = list(ds.iterate_entries())
    ent_names = [x.name for x in ents]
    assert set(ent_names) == {entry_name_1 + "new", entry_name_2 + "new", entry_name_3}
    assert set(ent_names) == set(ds.entry_names)
    assert set(ent_names) == set(ds._cache_data.get_entry_names())

    ds.fetch_records()
    assert ds._cache_data.get_dataset_record(entry_name_1, "spec_1") is None
    assert ds._cache_data.get_dataset_record(entry_name_2, "spec_1") is None

    assert ds._cache_data.get_dataset_record(entry_name_1 + "new", "spec_1").id == ent_rec_map[entry_name_1].id
    assert ds._cache_data.get_dataset_record(entry_name_2 + "new", "spec_1").id == ent_rec_map[entry_name_2].id
    assert ds._cache_data.get_dataset_record(entry_name_3, "spec_1").id == ent_rec_map[entry_name_3].id


def run_dataset_model_modify_entries(snowflake_client, ds, test_entries, test_specs):
    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries)
    ds.fetch_entries()

    entry_name_2 = test_entries[1].name

    expected_attribute_value = test_entries[1].attributes | {"test_attr_1": "val", "test_attr_2": 5}

    # Test Overwrite=False
    # Test modifying one entry attribute with no comments
    ds.modify_entries(attribute_map={entry_name_2: {"test_attr_1": "val", "test_attr_2": 5}})
    assert ds.get_entry(entry_name_2).attributes == expected_attribute_value

    expected_attribute_value.update({"test_attr_1": "new_val", "test_attr_2": 10})
    ds.modify_entries(attribute_map={entry_name_2: {"test_attr_1": "new_val", "test_attr_2": 10}})
    assert ds.get_entry(entry_name_2).attributes == expected_attribute_value

    # Test modifying both
    expected_attribute_value.update({"test_attr_1": "new_value", "test_attr_2": 19})
    ds.modify_entries(
        attribute_map={entry_name_2: {"test_attr_1": "new_value", "test_attr_2": 19}},
        comment_map={entry_name_2: "This is a new comment for the entry."},
    )
    assert ds.get_entry(entry_name_2).attributes == expected_attribute_value
    assert ds.get_entry(entry_name_2).comment == "This is a new comment for the entry."

    # Test Overwrite=True
    # Test modifying one entry attribute with no comments
    expected_attribute_value = {"test_attr_1": "val", "test_attr_2": 5}
    ds.modify_entries(attribute_map={entry_name_2: {"test_attr_1": "val", "test_attr_2": 5}}, overwrite_attributes=True)
    assert ds.get_entry(entry_name_2).attributes == expected_attribute_value

    # Test modifying one comment with no attributes
    ds.modify_entries(comment_map={entry_name_2: "This is a new comment tested without modifying attributes."})
    assert ds.get_entry(entry_name_2).attributes == expected_attribute_value
    assert ds.get_entry(entry_name_2).comment == "This is a new comment tested without modifying attributes."

    # Test modifying both
    expected_attribute_value = {"test_attr_1": "value"}
    ds.modify_entries(
        attribute_map={entry_name_2: {"test_attr_1": "value"}},
        comment_map={entry_name_2: "This is a new comment while overwriting the attributes."},
        overwrite_attributes=True,
    )
    assert ds.get_entry(entry_name_2).attributes == expected_attribute_value
    assert ds.get_entry(entry_name_2).comment == "This is a new comment while overwriting the attributes."

    # Now with a fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)
    ds.fetch_entries()
    assert ds.get_entry(entry_name_2).attributes == expected_attribute_value
    assert ds.get_entry(entry_name_2).comment == "This is a new comment while overwriting the attributes."


def run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs):
    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries)
    ds.submit()
    ds.fetch_records()

    ent_rec_map = {entry_name: record for entry_name, _, record in ds.iterate_records()}
    assert len(ent_rec_map) == 3

    entry_name_1 = test_entries[0].name
    entry_name_2 = test_entries[1].name
    entry_name_3 = test_entries[2].name

    for name in [entry_name_1, entry_name_2, entry_name_3]:
        assert name in ds.entry_names
        assert ds._cache_data.get_entry(name) is not None
        assert ds._cache_data.get_dataset_record(name, "spec_1") is not None

    meta = ds.delete_entries(entry_name_1, False)
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_children_deleted == 0
    snowflake_client.get_records(ent_rec_map[entry_name_1].id)  # exception if it doesn't exist

    meta = ds.delete_entries(entry_name_2, True)  # delete record, too
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_children_deleted == 1
    assert snowflake_client.get_records(ent_rec_map[entry_name_2].id, missing_ok=True) is None

    # Were removed from the model
    assert ds._cache_data.get_entry_names() == [entry_name_3]
    assert ds._entry_names == [entry_name_3]
    for name in [entry_name_1, entry_name_2]:
        assert name not in ds.entry_names
        assert ds._cache_data.get_entry(name) is None
        assert ds._cache_data.get_dataset_record(name, "spec_1") is None

    # Delete when it doesn't exist
    meta = ds.delete_entries(entry_name_2, True)
    assert meta.success is False
    assert meta.n_deleted == 0
    assert meta.n_children_deleted == 0
    assert meta.error_idx == [0]


def run_dataset_model_add_get_spec(
    snowflake_client,
    ds,
    test_specs,
):
    meta = ds.add_specification("spec_1", test_specs[0])
    assert meta.success
    assert meta.n_inserted == 1

    meta = ds.add_specification("spec_2", test_specs[1], description="a description")
    assert meta.success
    assert meta.n_inserted == 1

    assert set(ds._specification_names) == {"spec_1", "spec_2"}
    assert set(ds.specifications.keys()) == {"spec_1", "spec_2"}

    # Now with a fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert set(ds.specifications.keys()) == {"spec_1", "spec_2"}

    assert ds.specifications["spec_1"].name == "spec_1"
    assert ds.specifications["spec_1"].specification == test_specs[0]
    assert ds.specifications["spec_1"].description is None

    assert ds.specifications["spec_2"].name == "spec_2"

    assert ds.specifications["spec_2"].specification == test_specs[1]
    assert ds.specifications["spec_2"].description == "a description"


def run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs):
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
    ds = snowflake_client.get_dataset_by_id(ds.id)
    assert set(ds.specifications.keys()) == {"spec_1"}

    assert ds.specifications["spec_1"].name == "spec_1"
    assert ds.specifications["spec_1"].specification == test_specs[0]
    assert ds.specifications["spec_1"].description is None


def run_dataset_model_rename_spec(snowflake_client, ds, test_entries, test_specs):
    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    entry_name = test_entries[0].name
    ds.add_entries(test_entries[0])
    ds.submit()
    ds.fetch_records()

    spec_rec_map = {spec_name: record for _, spec_name, record in ds.iterate_records()}
    assert len(spec_rec_map) == 2

    ds.rename_specification("spec_1", "spec_1_new")

    assert set(ds.specifications.keys()) == {"spec_2", "spec_1_new"}
    assert set(ds._specification_names) == {"spec_2", "spec_1_new"}
    assert set(ds._cache_data.get_specification_names()) == {"spec_2", "spec_1_new"}
    assert ds._cache_data.get_specification("spec_1_new").name == "spec_1_new"

    with pytest.raises(PortalRequestError, match=r"Missing.*spec"):
        ds.get_record(entry_name, "spec_1")

    assert ds._cache_data.get_dataset_record(entry_name, "spec_1") is None

    assert ds.get_record(entry_name, "spec_1_new").id == spec_rec_map["spec_1"].id
    assert ds._cache_data.get_dataset_record(entry_name, "spec_1_new").id == spec_rec_map["spec_1"].id

    # Now with a fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)
    specs = ds.specifications.values()
    spec_names = [x.name for x in specs]

    assert set(spec_names) == {"spec_2", "spec_1_new"}
    assert set(ds.specifications.keys()) == {"spec_2", "spec_1_new"}
    assert set(ds.specification_names) == {"spec_2", "spec_1_new"}
    assert set(ds._specification_names) == {"spec_2", "spec_1_new"}
    assert set(ds._cache_data.get_specification_names()) == {"spec_2", "spec_1_new"}
    assert ds._cache_data.get_specification("spec_1_new").name == "spec_1_new"

    with pytest.raises(PortalRequestError, match=r"Missing.*spec"):
        ds.get_record(entry_name, "spec_1")

    assert ds._cache_data.get_dataset_record(entry_name, "spec_1") is None

    assert ds.get_record(entry_name, "spec_1_new").id == spec_rec_map["spec_1"].id
    assert ds._cache_data.get_dataset_record(entry_name, "spec_1_new").id == spec_rec_map["spec_1"].id


def run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs):
    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries[0])
    ds.submit()
    ds.fetch_records()

    entry_name = test_entries[0].name

    for name in ["spec_1", "spec_2"]:
        assert ds._cache_data.get_specification(name) is not None
        assert ds._cache_data.get_dataset_record(entry_name, name) is not None

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
    assert ds._cache_data.get_specification_names() == []
    assert ds._specification_names == []
    assert ds.specification_names == []
    assert ds.specifications == {}

    for name in ["spec_1", "spec_2"]:
        assert ds._cache_data.get_specification(name) is None
        assert ds._cache_data.get_dataset_record(name, entry_name) is None

    # Delete when it doesn't exist
    meta = ds.delete_specification("spec_1", True)
    assert meta.success is False
    assert meta.n_deleted == 0
    assert meta.n_children_deleted == 0
    assert meta.error_idx == [0]


def run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs):
    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries)
    ds.submit()

    all_records = list(ds.iterate_records())
    assert len(all_records) == 6

    rec_ids = [r.id for _, _, r in all_records]
    entry_name_1 = test_entries[0].name
    entry_name_2 = test_entries[2].name

    ds.remove_records([entry_name_1, entry_name_2], ["spec_1"], delete_records=False)
    all_records_2 = list(ds.iterate_records())
    assert len(all_records_2) == 4

    # Record ids should exist in the db
    snowflake_client.get_records(rec_ids)

    # Now with a fresh dataset
    ds = snowflake_client.get_dataset_by_id(ds.id)
    all_records_2 = list(ds.iterate_records())
    assert len(all_records_2) == 4

    # Delete records as well
    to_delete_id = ds.get_record(entry_name_1, "spec_2").id
    ds.remove_records(entry_name_1, "spec_2", delete_records=True)
    all_records_2 = list(ds.iterate_records())
    assert len(all_records_2) == 3

    # Should be one missing
    recs = snowflake_client.get_records(rec_ids, missing_ok=True)
    assert recs.count(None) == 1
    none_idx = recs.index(None)
    assert rec_ids[none_idx] == to_delete_id


def run_dataset_model_submit(ds, test_entries, test_spec, record_compare, background):
    assert ds.record_count == 0
    assert ds._client.list_datasets()[0]["record_count"] == 0

    ds.add_specification("spec_1", test_spec)
    ds.add_entries(test_entries[0])

    if background:
        ij = ds.background_submit()
        ij.watch(interval=0.1, timeout=10)
        meta = InsertCountsMetadata(**ij.result)
    else:
        meta = ds.submit()

    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0

    all_records = list(ds.iterate_records())
    assert len(all_records) == 1
    rec = all_records[0][2]
    assert rec.status == RecordStatusEnum.waiting

    record_compare(rec, test_entries[0], test_spec)

    assert rec.owner_user == "submit_user"
    assert rec.owner_group == "group1"

    # Used default tag/priority
    if rec.is_service:
        assert rec.service.compute_tag == "default_tag"
        assert rec.service.compute_priority == PriorityEnum.low
    else:
        assert rec.task.compute_tag == "default_tag"
        assert rec.task.compute_priority == PriorityEnum.low

    # Now additional keywords
    ds.add_entries(test_entries[2])

    if background:
        ij = ds.background_submit()
        ij.watch(interval=0.1, timeout=10)
        meta = InsertCountsMetadata(**ij.result)
    else:
        meta = ds.submit()

    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0

    rec = ds.get_record(test_entries[2].name, "spec_1")
    record_compare(rec, test_entries[2], test_spec)

    # Additional submission stuff
    ds.add_entries(test_entries[1])

    if background:
        ij = ds.background_submit(compute_tag="new_tag", compute_priority=PriorityEnum.high)
        ij.watch(interval=0.1, timeout=10)
        meta = InsertCountsMetadata(**ij.result)
    else:
        meta = ds.submit(compute_tag="new_tag", compute_priority=PriorityEnum.high)

    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0

    rec = ds.get_record(test_entries[1].name, "spec_1")

    if rec.is_service:
        assert rec.service.compute_tag == "new_tag"
        assert rec.service.compute_priority == PriorityEnum.high
    else:
        assert rec.task.compute_tag == "new_tag"
        assert rec.task.compute_priority == PriorityEnum.high

    # But didn't change others
    rec = ds.get_record(test_entries[2].name, "spec_1")

    if rec.is_service:
        assert rec.service.compute_tag == "default_tag"
        assert rec.service.compute_priority == PriorityEnum.low
    else:
        assert rec.task.compute_tag == "default_tag"
        assert rec.task.compute_priority == PriorityEnum.low

    # Find existing, but not already attached
    old_rec_id = rec.id
    ds.remove_records(test_entries[2].name, "spec_1")

    if background:
        ij = ds.background_submit()
        ij.watch(interval=0.1, timeout=10)
        meta = InsertCountsMetadata(**ij.result)
    else:
        meta = ds.submit()

    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 1

    rec = ds.get_record(test_entries[2].name, "spec_1")
    assert rec.id == old_rec_id

    record_count = len(ds.entry_names) * len(ds.specifications)
    assert ds.record_count == record_count
    assert ds._client.list_datasets()[0]["record_count"] == record_count

    # Don't find existing
    old_rec_id = rec.id
    ds.remove_records(test_entries[2].name, "spec_1")

    if background:
        ij = ds.background_submit(find_existing=False)
        ij.watch(interval=0.1, timeout=10)
        meta = InsertCountsMetadata(**ij.result)
    else:
        meta = ds.submit(find_existing=False)

    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0

    rec = ds.get_record(test_entries[2].name, "spec_1")
    assert rec.id != old_rec_id

    record_count = len(ds.entry_names) * len(ds.specifications)
    assert ds.record_count == record_count
    assert ds._client.list_datasets()[0]["record_count"] == record_count


def run_dataset_model_copy_full(snowflake_client, dataset_type, test_entries, test_specs, entry_extra_compare):
    ds1 = snowflake_client.add_dataset(dataset_type, "Test dataset 1")
    ds2 = snowflake_client.add_dataset(dataset_type, "Test dataset 2")
    ds3 = snowflake_client.add_dataset(dataset_type, "Test dataset 3")
    ds4 = snowflake_client.add_dataset(dataset_type, "Test dataset 4")
    ds1.add_specification("spec_1", test_specs[0])
    ds1.add_specification("spec_2", test_specs[1])
    ds1.add_entries(test_entries)
    ds1.submit()

    #################
    # Copy all to ds2
    #################
    ds2.copy_records_from(ds1.id)
    ds2 = snowflake_client.get_dataset_by_id(ds2.id)

    with pytest.raises(PortalRequestError, match="already has specifications with the same name"):
        ds2.copy_records_from(ds1.id)

    for e in ds1.iterate_entries():
        e2 = ds2.get_entry(e.name)
        _compare_entries(e, e2, entry_extra_compare)

    assert ds1.specifications == ds2.specifications

    for e, s, r in ds1.iterate_records():
        r2 = ds2.get_record(e, s)
        assert r.id == r2.id  # only really need to check ids

    ###########################
    # Copy only one spec to ds3
    ###########################
    ds3.copy_records_from(ds1.id, specification_names=["spec_1"])

    with pytest.raises(PortalRequestError, match="already has specifications with the same name"):
        ds3.copy_records_from(ds1.id, specification_names=["spec_1"])

    ds3 = snowflake_client.get_dataset_by_id(ds3.id)

    for e in ds1.iterate_entries():
        e3 = ds3.get_entry(e.name)
        _compare_entries(e, e3, entry_extra_compare)

    assert len(ds3.specifications) == 1
    assert ds1.specifications["spec_1"] == ds3.specifications["spec_1"]

    for e, s, r3 in ds3.iterate_records():
        r = ds1.get_record(e, s)
        assert r.id == r3.id  # only really need to check ids

    #################################
    # Only one spec and entry to ds3
    #################################
    ename = ds1.entry_names[0]
    ds4.copy_records_from(ds1.id, entry_names=[ename], specification_names=["spec_1"])

    ds4 = snowflake_client.get_dataset_by_id(ds4.id)
    assert len(ds4.entry_names) == 1

    e = ds1.get_entry(ename)
    e4 = ds4.get_entry(ename)
    _compare_entries(e, e4, entry_extra_compare)

    assert len(ds3.specifications) == 1
    assert ds1.specifications["spec_1"] == ds3.specifications["spec_1"]

    r4 = ds4.get_record(ename, "spec_1")
    r = ds1.get_record(ename, "spec_1")
    assert r.id == r4.id

    #################################
    # Test duplicate entry finding
    #################################
    ds5 = snowflake_client.add_dataset(dataset_type, "Test dataset 5")
    ds5.add_entries(test_entries)

    with pytest.raises(PortalRequestError, match="already has entries with the same name"):
        ds5.copy_records_from(ds1.id)


def run_dataset_model_copy(snowflake_client, dataset_type, test_entries, test_specs, entry_extra_compare):
    ds1 = snowflake_client.add_dataset(dataset_type, "Test dataset 1")
    ds2 = snowflake_client.add_dataset(dataset_type, "Test dataset 2")
    ds3 = snowflake_client.add_dataset(dataset_type, "Test dataset 3")
    ds4 = snowflake_client.add_dataset(dataset_type, "Test dataset 4")
    ds1.add_specification("spec_1", test_specs[0])
    ds1.add_specification("spec_2", test_specs[1])
    ds1.add_entries(test_entries)

    #################################################
    # Copy all entries and specs to ds2
    #################################################
    ds2.copy_entries_from(ds1.id)
    assert set(ds2.entry_names) == set(ds1.entry_names)
    assert len(ds2.specifications) == 0

    for e in ds1.iterate_entries():
        e2 = ds2.get_entry(e.name)
        _compare_entries(e, e2, entry_extra_compare)

    ds2.copy_specifications_from(ds1.id)
    assert ds1.specifications == ds2.specifications

    all_recs = [(e, s, r) for e, s, r in ds2.iterate_records()]
    assert len(all_recs) == 0

    #################################################
    # Selectively copy spec and entry
    #################################################
    ename = ds1.entry_names[0]
    ds3.copy_entries_from(ds1.id, entry_names=ename)  # not using lists on purpose
    ds3.copy_specifications_from(ds1.id, specification_names="spec_1")

    assert len(ds3.specifications) == 1
    assert ds1.specifications["spec_1"] == ds3.specifications["spec_1"]
    assert len(ds3.entry_names) == 1

    e = ds1.get_entry(ename)
    e3 = ds3.get_entry(ename)
    _compare_entries(e, e3, entry_extra_compare)

    # records not copied
    all_recs = [(e, s, r) for e, s, r in ds2.iterate_records()]
    assert len(all_recs) == 0


def run_dataset_model_clone(snowflake_client, dataset_type, test_entries, test_specs, entry_extra_compare):
    ds = snowflake_client.add_dataset(dataset_type, "Test dataset")
    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries)
    ds.submit()

    ds2 = snowflake_client.clone_dataset(ds.id, "Test dataset 2")

    assert ds2.id != ds.id
    assert ds2.dataset_type == ds.dataset_type == dataset_type
    assert set(ds.entry_names) == set(ds2.entry_names)
    assert set(ds.specifications.keys()) == set(ds2.specifications.keys())

    for e in ds.iterate_entries():
        e2 = ds2.get_entry(e.name)
        _compare_entries(e, e2, entry_extra_compare)

    for k, v in ds.specifications.items():
        v2 = ds2.specifications[k]
        assert v == v2

    for e, s, r in ds.iterate_records():
        r2 = ds2.get_record(e, s)
        assert r.id == r2.id  # only really need to check ids


def run_dataset_model_submit_missing(ds, test_entries, test_spec):
    ds.add_specification("spec_1", test_spec)
    ds.add_entries(test_entries)
    ds.submit()  # should be ok

    with pytest.raises(PortalRequestError, match="Could not find all entries"):
        ds.submit(entry_names="non_entry_1")
    with pytest.raises(PortalRequestError, match="Could not find all specifications"):
        ds.submit(specification_names="non_spec_1")


def run_dataset_model_iterate_updated(snowflake_client, ds, test_entries, test_spec):
    ds.add_specification("spec_1", test_spec)
    ds.add_entries(test_entries)
    ds.submit()

    assert all(x.status == RecordStatusEnum.waiting for _, _, x in ds.iterate_records())
    entry_name = test_entries[0].name

    time_0 = now_at_utc()
    rid = ds.get_record(entry_name, "spec_1").id
    snowflake_client.cancel_records(rid)
    time_1 = now_at_utc()

    # Should be automatically updated when we iterate
    all_records = list(ds.iterate_records())
    cancelled = [(e, s, r) for e, s, r in all_records if r.status == RecordStatusEnum.cancelled]
    assert len(cancelled) == 1
    assert cancelled[0][0] == entry_name
    assert cancelled[0][1] == "spec_1"
    assert time_0 < cancelled[0][2].modified_on < time_1

    # Do another one
    entry_name = test_entries[1].name
    rid = ds.get_record(entry_name, "spec_1").id
    snowflake_client.cancel_records(rid)

    # Disable auto updating
    # First, we need to sync the existing records to the cache
    # Otherwise, the records may not exist in the cache and will be fetched fresh from the server
    for _, _, r in all_records:
        r.sync_to_cache(True)
    all_records = list(ds.iterate_records(fetch_updated=False))
    cancelled = [(e, s, r) for e, s, r in all_records if r.status == RecordStatusEnum.cancelled]
    assert len(cancelled) == 1  # did not fetch the newly-cancelled one

    # Now force update
    all_records = list(ds.iterate_records(fetch_updated=False, force_refetch=True))
    cancelled = [(e, s, r) for e, s, r in all_records if r.status == RecordStatusEnum.cancelled]
    assert len(cancelled) == 2  # fetched them all


def run_dataset_model_modify_records(ds, test_entries, test_spec):
    ds.add_specification("spec_1", test_spec)
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
        entry_name,
        spec_name,
        new_compute_tag="new_tag",
        new_compute_priority=PriorityEnum.low,
        new_comment="a new comment",
    )
    rec = ds.get_record(entry_name, spec_name)
    assert rec.status == RecordStatusEnum.waiting

    if rec.is_service:
        assert rec.service.compute_tag == "new_tag"
        assert rec.service.compute_priority == PriorityEnum.low
    else:
        assert rec.task.compute_tag == "new_tag"
        assert rec.task.compute_priority == PriorityEnum.low

    assert rec.comments[0].comment == "a new comment"
