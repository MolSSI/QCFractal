from __future__ import annotations

import pytest

from qcfractal.components.records.testing_helpers import populate_records_status
from qcfractal.testing_helpers import TestingSnowflake
from qcportal import PortalClient
from qcportal.molecules import Molecule
from qcportal.records import RecordStatusEnum


@pytest.fixture(scope="module")
def queryable_records_client(module_temporary_database):
    db_config = module_temporary_database.config
    with TestingSnowflake(db_config, encoding="application/json") as server:

        # First populate all the statuses
        populate_records_status(server.get_storage_socket())

        # Now a bunch of records
        client = server.client()

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

        assert len(all_ids) == 320
        yield client


def test_record_client_query(queryable_records_client: PortalClient):
    query_res = queryable_records_client.query_records(record_type="singlepoint")
    assert query_res.current_meta.n_found == 325

    # Get some ids from the last query
    all_records = list(query_res)
    ids = [x.id for x in all_records]
    query_res = queryable_records_client.query_records(record_id=ids[5:15])
    assert query_res.current_meta.n_found == 10

    # Created/modified before/after
    sorted_records = sorted(all_records, key=lambda x: x.created_on)
    query_res = queryable_records_client.query_records(
        record_type="singlepoint", created_before=sorted_records[73].created_on
    )
    assert query_res.current_meta.n_found == 74

    query_res = queryable_records_client.query_records(
        record_type="singlepoint", created_after=sorted_records[73].created_on
    )
    assert query_res.current_meta.n_found == 252

    sorted_records = sorted(all_records, key=lambda x: x.modified_on)
    query_res = queryable_records_client.query_records(
        record_type="singlepoint", modified_before=sorted_records[73].created_on
    )
    assert query_res.current_meta.n_found == 73

    query_res = queryable_records_client.query_records(
        record_type="singlepoint", modified_after=sorted_records[73].created_on
    )
    assert query_res.current_meta.n_found == 252

    # find the manager used to compute these
    manager_res = queryable_records_client.query_managers()
    manager = list(manager_res)[0]
    query_res = queryable_records_client.query_records(manager_name=manager.name)
    assert query_res.current_meta.n_found == 4

    # Querying based on status
    query_res = queryable_records_client.query_records(status=[RecordStatusEnum.error])
    assert query_res.current_meta.n_found == 1

    query_res = queryable_records_client.query_records(status=RecordStatusEnum.cancelled)
    assert query_res.current_meta.n_found == 1

    query_res = queryable_records_client.query_records(status=[RecordStatusEnum.error, RecordStatusEnum.deleted])
    assert query_res.current_meta.n_found == 2

    # Some combinations
    query_res = queryable_records_client.query_records(record_type=["singlepoint"], status=[RecordStatusEnum.waiting])
    assert query_res.current_meta.n_found == 320

    query_res = queryable_records_client.query_records(record_type=["optimization"], status=[RecordStatusEnum.error])
    assert query_res.current_meta.n_found == 1

    # Including fields
    query_res = queryable_records_client.query_records(status=RecordStatusEnum.error)
    recs = list(query_res)
    assert query_res.current_meta.n_found == 1
    assert recs[0].raw_data.task is None
    assert recs[0].raw_data.compute_history[0].outputs is None

    query_res = queryable_records_client.query_records(status=RecordStatusEnum.error, include=["outputs", "task"])
    recs = list(query_res)
    assert query_res.current_meta.n_found == 1
    assert recs[0].raw_data.task is not None
    assert recs[0].raw_data.compute_history[0].outputs is not None


def test_record_client_query_empty_iter(queryable_records_client: PortalClient):
    # Empty query
    query_res = queryable_records_client.query_records()
    assert len(query_res.current_batch) < queryable_records_client.api_limits["get_records"]

    all_recs = list(query_res)
    assert len(all_recs) == 327


def test_record_client_query_limit(queryable_records_client: PortalClient):

    query_res = queryable_records_client.query_records(record_type="singlepoint", limit=50)

    assert query_res.current_meta.success
    assert query_res.current_meta.n_found == 325

    all_recs = list(query_res)
    assert len(all_recs) == 50
