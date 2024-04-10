from __future__ import annotations

import gc
import threading
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from qcportal.dataset_models import load_dataset_view
from qcportal.record_models import RecordStatusEnum
from qcportal.singlepoint import SinglepointDataset
from qcportal.singlepoint.test_dataset_models import test_specs, test_entries

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_dataset_cache_basic(snowflake: QCATestingSnowflake, tmp_path):
    cache_dir = tmp_path / "ptlcache"
    client = snowflake.client(cache_dir=str(cache_dir))
    ds: SinglepointDataset = client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries[0])
    ds.add_entries(test_entries[1])
    ds.submit()
    ds.fetch_records(include=["molecule"])

    # reload the dataset
    ds: SinglepointDataset = client.get_dataset("singlepoint", "Test dataset")
    ds.propagate_client(None)

    spec_map = {"spec_1": test_specs[0], "spec_2": test_specs[1]}
    entry_map = {test_entries[0].name: test_entries[0], test_entries[1].name: test_entries[1]}
    spec_names = {"spec_1", "spec_2"}
    entry_names = {test_entries[0].name, test_entries[1].name}

    # Specifications
    assert set(ds._cache_data.get_specification_names()) == spec_names
    cache_specs = ds._cache_data.get_all_specifications()
    assert set(x.name for x in cache_specs) == spec_names

    cache_specs = ds._cache_data.get_specifications(spec_names)
    assert set(x.name for x in cache_specs) == spec_names

    for sname, s in spec_map.items():
        assert ds._cache_data.specification_exists(sname)
        full_spec = ds._cache_data.get_specification(sname)
        assert full_spec.name == sname
        assert full_spec.specification == s

    # Entries
    assert set(ds._cache_data.get_entry_names()) == entry_names
    cache_entries = ds._cache_data.get_entries(entry_names)
    assert set(x.name for x in cache_entries) == entry_names

    for ename, e in entry_map.items():
        assert ds._cache_data.entry_exists(ename)
        full_entry = ds._cache_data.get_entry(ename)
        assert full_entry.name == e.name == ename
        assert full_entry.molecule == e.molecule

    # Records
    for ename, e in entry_map.items():
        for sname, s in spec_map.items():
            assert ds._cache_data.dataset_record_exists(ename, sname)
            r = ds._cache_data.get_dataset_record(ename, sname)
            assert r is not None
            assert r.specification == s
            assert r.molecule == e.molecule

            ri = ds._cache_data.get_dataset_record_info([ename], [sname], None)
            assert len(ri) == 1

            ri = ds._cache_data.get_dataset_record_info([ename], [sname], [RecordStatusEnum.waiting])
            assert len(ri) == 1

            assert ri[0][0] == ename
            assert ri[0][1] == sname
            assert ri[0][2] == r.id
            assert ri[0][3] == r.status
            assert ri[0][4] == r.modified_on

    recs = ds._cache_data.get_dataset_records(entry_names, spec_names)
    assert len(recs) == len(entry_names) * len(spec_names)

    rinfo = ds._cache_data.get_dataset_record_info(entry_names, spec_names, [RecordStatusEnum.waiting])
    assert len(rinfo) == len(entry_names) * len(spec_names)

    rinfo = ds._cache_data.get_dataset_record_info(
        entry_names, spec_names, [RecordStatusEnum.waiting, RecordStatusEnum.running]
    )
    assert len(rinfo) == len(entry_names) * len(spec_names)

    rinfo = ds._cache_data.get_dataset_record_info(entry_names, spec_names, [RecordStatusEnum.complete])
    assert len(rinfo) == 0


def test_dataset_cache_update(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries[0])
    ds.add_entries(test_entries[1])
    ds.submit()

    r = ds.get_record(test_entries[0].name, "spec_1")
    snowflake_client.cancel_records(r.id)
    del r

    # Cancel via client itself, not dataset

    r = ds.get_record(test_entries[0].name, "spec_1")
    assert r.status == RecordStatusEnum.cancelled
    del r

    r2 = ds._cache_data.get_dataset_record(test_entries[0].name, "spec_1")
    assert r2.status == RecordStatusEnum.cancelled
    del r2

    # Test via iteration
    r = ds.get_record(test_entries[1].name, "spec_2")
    snowflake_client.cancel_records(r.id)
    del r

    for e, s, r in ds.iterate_records():
        if e == test_entries[1].name and s == "spec_2":
            assert r.status == RecordStatusEnum.cancelled


def test_dataset_cache_multithread(snowflake: QCATestingSnowflake):
    snowflake_client: PortalClient = snowflake.client()

    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries[0])
    ds.add_entries(test_entries[1])
    ds.submit()

    success = True

    def _test_thread():
        nonlocal success

        try:
            records = list(ds.iterate_records())
            assert len(records) == 4
        except Exception as e:
            print(threading.get_ident(), "EXCEPTION: ", str(e))
            success = False
            raise

    threads = [threading.Thread(target=_test_thread) for _ in range(1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    records = list(ds.iterate_records())
    assert len(records) == 4

    assert success


def test_dataset_cache_writeback(snowflake_client: PortalClient):
    ds: SinglepointDataset = snowflake_client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries[0])
    ds.submit()
    ds.fetch_records()

    r = ds.get_record(test_entries[0].name, "spec_1")
    assert r.molecule_ is None  # molecule not fetched
    r._fetch_molecule()
    assert r.molecule_ is not None  # molecule not fetched

    # TODO: This is a hack to force a writeback. Remove when proper tracking of _cache_dirty is done in the records
    r._cache_dirty = True
    del r  # should write back to the cache
    gc.collect()

    r3 = ds._cache_data.get_dataset_record(test_entries[0].name, "spec_1")
    assert r3.molecule_ is not None

    # the record with the missing molecule doesn't get written back
    r3.molecule_ = None
    del r3
    gc.collect()

    r3 = ds._cache_data.get_dataset_record(test_entries[0].name, "spec_1")
    assert r3.molecule_ is not None


def test_dataset_cache_fromfile(snowflake: QCATestingSnowflake, tmp_path):
    cache_dir = tmp_path / "ptlcache"
    client = snowflake.client(cache_dir=str(cache_dir))
    ds: SinglepointDataset = client.add_dataset("singlepoint", "Test dataset")
    ds_id = ds.id

    ds.add_specification("spec_1", test_specs[0])
    ds.add_specification("spec_2", test_specs[1])
    ds.add_entries(test_entries[0])
    ds.add_entries(test_entries[1])
    ds.submit()
    ds.fetch_records()
    rid = ds.get_record(test_entries[0].name, "spec_1").id

    cachefile_path = urlparse(ds._cache_data.cache_uri).path

    del ds, client
    gc.collect()

    client = snowflake.client()  # memory cache
    # Cancel one of the records

    client.cancel_records(rid)

    ds2 = client.dataset_from_cache(cachefile_path)
    assert ds2.id == ds_id

    r2 = ds2._cache_data.get_dataset_record(test_entries[0].name, "spec_2")
    assert r2 is not None

    r3 = ds2.get_record(test_entries[0].name, "spec_2")
    assert r3 is not None

    r3 = ds2.get_record(test_entries[0].name, "spec_1")
    assert r3.status == RecordStatusEnum.cancelled  # was updated from the server


def test_dataset_cache_fromfile_deleted(snowflake: QCATestingSnowflake, tmp_path):
    cache_dir = tmp_path / "ptlcache"
    client = snowflake.client(cache_dir=str(cache_dir))
    ds: SinglepointDataset = client.add_dataset("singlepoint", "Test dataset")
    ds_id = ds.id

    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries[0])
    ds.submit()
    ds.fetch_records()

    cachefile_path = urlparse(ds._cache_data.cache_uri).path

    del ds
    gc.collect()

    client.delete_dataset(ds_id, True)

    ds2 = client.dataset_from_cache(cachefile_path)

    assert ds2.is_view
    assert ds2._cache_data.read_only is True

    assert ds2.get_record(test_entries[0].name, "spec_1") is not None


def test_dataset_cache_fromfile_view(snowflake: QCATestingSnowflake, tmp_path):
    cache_dir = tmp_path / "ptlcache"
    client = snowflake.client(cache_dir=str(cache_dir))
    ds: SinglepointDataset = client.add_dataset("singlepoint", "Test dataset")

    ds.add_specification("spec_1", test_specs[0])
    ds.add_entries(test_entries[0])
    ds.submit()
    ds.fetch_records()

    cachefile_path = urlparse(ds._cache_data.cache_uri).path

    del ds
    gc.collect()

    ds2 = load_dataset_view(cachefile_path)
    assert ds2.is_view
    assert ds2.get_record(test_entries[0].name, "spec_1") is not None
