from __future__ import annotations

import time
from typing import Optional

import pytest
import pytz

from qcfractal.components.testing_helpers import populate_records_status
from qcportal import PortalClient
from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum


@pytest.fixture(scope="module")
def queryable_records_client(session_snowflake):
    # First populate all the statuses
    populate_records_status(session_snowflake.get_storage_socket())

    # Now a bunch of records
    client = session_snowflake.client()

    elements = ["h", "he", "li", "be", "b", "c", "n", "o", "f", "ne"]

    all_mols = []
    for el in elements:
        m = Molecule(
            symbols=[el],
            geometry=[0, 0, 0],
            identifiers={
                "smiles": f"madeupsmiles_{el}",
                "inchikey": f"madeupinchi_{el}",
            },
        )
        all_mols.append(m)

    all_ids = []
    for prog in ["prog1", "prog2"]:
        for driver in ["energy", "properties"]:
            for method in ["hf", "b3lyp"]:
                for basis in ["sto-3g", "def2-tzvp"]:
                    for kw in [{"maxiter": 100}, None]:
                        meta, ids = client.add_singlepoints(all_mols, prog, driver, method, basis, kw)
                        assert meta.success
                        all_ids.extend(ids)

        # prevents spurious test errors. On fast machines,
        # records can be created too close together
        # (we test at this boundary)
        time.sleep(0.05)

    assert len(all_ids) == 320
    yield client
    session_snowflake.reset()


def test_record_client_query(queryable_records_client: PortalClient):
    query_res = queryable_records_client.query_records(record_type="singlepoint")
    all_records = list(query_res)
    assert len(all_records) == 325

    # Get some ids from the last query
    ids = [x.id for x in all_records]
    query_res = queryable_records_client.query_records(record_id=ids[5:15])
    query_res_l = list(query_res)
    assert len(query_res_l) == 10

    # Created/modified before/after
    sorted_records = sorted(all_records, key=lambda x: x.created_on)
    query_res = queryable_records_client.query_records(
        record_type="singlepoint", created_before=sorted_records[164].created_on
    )
    query_res_l = list(query_res)
    assert len(query_res_l) == 165

    query_res = queryable_records_client.query_records(
        record_type="singlepoint", created_after=sorted_records[165].created_on
    )
    query_res_l = list(query_res)
    assert len(query_res_l) == 160

    sorted_records = sorted(all_records, key=lambda x: x.modified_on)
    query_res = queryable_records_client.query_records(
        record_type="singlepoint", modified_before=sorted_records[165].modified_on
    )
    query_res_l = list(query_res)
    assert len(query_res_l) == 166

    query_res = queryable_records_client.query_records(
        record_type="singlepoint", modified_after=sorted_records[165].modified_on
    )
    query_res_l = list(query_res)
    assert len(query_res_l) == 160

    # find the manager used to compute these
    manager_res = queryable_records_client.query_managers()
    manager = list(manager_res)[0]
    query_res = queryable_records_client.query_records(manager_name=manager.name)
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    # Querying based on status
    query_res = queryable_records_client.query_records(status=[RecordStatusEnum.error])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = queryable_records_client.query_records(status=RecordStatusEnum.cancelled)
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    query_res = queryable_records_client.query_records(status=[RecordStatusEnum.error, RecordStatusEnum.deleted])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # Some combinations
    query_res = queryable_records_client.query_records(record_type=["singlepoint"], status=[RecordStatusEnum.waiting])
    query_res_l = list(query_res)
    assert len(query_res_l) == 320

    query_res = queryable_records_client.query_records(record_type=["optimization"], status=[RecordStatusEnum.error])
    query_res_l = list(query_res)
    assert len(query_res_l) == 1

    # Including fields
    query_res = queryable_records_client.query_records(status=RecordStatusEnum.error)
    recs = list(query_res)
    assert len(recs) == 1
    assert recs[0].task_ is None
    assert recs[0].compute_history[0].outputs_ is None

    query_res = queryable_records_client.query_records(
        status=RecordStatusEnum.error, include=["compute_history", "task"]
    )
    recs = list(query_res)
    assert recs[0].task_ is not None
    assert recs[0].compute_history_ is not None


@pytest.mark.parametrize("timezone", [None, "UTC", "ETC/UTC", "America/New_York", "Asia/Singapore"])
def test_record_client_query_timezones(queryable_records_client: PortalClient, timezone: Optional[str]):
    tzinfo = pytz.timezone(timezone) if timezone is not None else None
    query_res = queryable_records_client.query_records(record_type="singlepoint")
    all_records = list(query_res)
    assert len(all_records) == 325

    sorted_records = sorted(all_records, key=lambda x: x.created_on)
    created_before = sorted_records[164].created_on.astimezone(tzinfo)
    # print("CREATED BEFORE", created_before)
    query_res = queryable_records_client.query_records(record_type="singlepoint", created_before=created_before)
    query_res_l = list(query_res)
    assert len(query_res_l) == 165

    created_after = sorted_records[165].created_on.astimezone(tzinfo)
    # print("CREATED AFTER", created_after)
    query_res = queryable_records_client.query_records(record_type="singlepoint", created_after=created_after)
    query_res_l = list(query_res)
    assert len(query_res_l) == 160

    sorted_records = sorted(all_records, key=lambda x: x.modified_on)
    modified_before = sorted_records[165].modified_on.astimezone(tzinfo)
    # print("MODIFIED BEFORE", modified_before)
    query_res = queryable_records_client.query_records(record_type="singlepoint", modified_before=modified_before)
    query_res_l = list(query_res)
    assert len(query_res_l) == 166

    modified_after = sorted_records[165].modified_on.astimezone(tzinfo)
    # print("MODIFIED AFTER", modified_after)
    query_res = queryable_records_client.query_records(record_type="singlepoint", modified_after=modified_after)
    query_res_l = list(query_res)
    assert len(query_res_l) == 160


def test_record_client_query_empty_iter(queryable_records_client: PortalClient):
    # Empty query
    query_res = queryable_records_client.query_records()
    assert len(query_res._current_batch) < queryable_records_client.api_limits["get_records"]

    all_recs = list(query_res)
    assert len(all_recs) == 327


def test_record_client_query_limit(queryable_records_client: PortalClient):
    query_res = queryable_records_client.query_records(record_type="singlepoint", limit=100)
    all_recs = list(query_res)
    assert len(all_recs) == 100

    query_res = queryable_records_client.query_records(record_type="singlepoint", limit=50)
    all_recs = list(query_res)
    assert len(all_recs) == 50
