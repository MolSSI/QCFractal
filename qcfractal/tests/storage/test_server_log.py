"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

from datetime import datetime
from time import time

import numpy as np
import pytest
import sqlalchemy
import sqlalchemy.exc

import qcfractal.interface as ptl
from qcfractal.interface.models import TaskStatusEnum
from qcfractal.services.services import TorsionDriveService


@pytest.fixture(scope="function")
def storage_results(storage_socket):
    mol_names = [
        "water_dimer_minima.psimol",
        "water_dimer_stretch.psimol",
        "water_dimer_stretch2.psimol",
        "neon_tetramer.psimol",
    ]

    molecules = []
    for mol_name in mol_names:
        mol = ptl.data.get_molecule(mol_name)
        molecules.append(mol)

    meta, mol_insert = storage_socket.molecule.add(molecules)
    assert meta.success

    kw1 = ptl.models.KeywordSet(**{"values": {}})
    kwid1 = storage_socket.keywords.add([kw1])[1][0]

    page1 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert[0],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "energy",
            "return_result": 5,
            "hash_index": 0,
            "status": "COMPLETE",
        }
    )

    page2 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert[1],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "energy",
            "return_result": 10,
            "hash_index": 1,
            "status": "COMPLETE",
        }
    )

    page3 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert[0],
            "method": "M1",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P2",
            "driver": "gradient",
            "return_result": 15,
            "hash_index": 2,
            "status": "COMPLETE",
        }
    )

    page4 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert[0],
            "method": "M2",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P2",
            "driver": "gradient",
            "return_result": 15,
            "hash_index": 3,
            "status": "COMPLETE",
        }
    )

    page5 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert[1],
            "method": "M2",
            "basis": "B1",
            "keywords": kwid1,
            "program": "P1",
            "driver": "gradient",
            "return_result": 20,
            "hash_index": 4,
            "status": "COMPLETE",
        }
    )

    page6 = ptl.models.ResultRecord(
        **{
            "molecule": mol_insert[1],
            "method": "M3",
            "basis": "B1",
            "keywords": None,
            "program": "P1",
            "driver": "gradient",
            "return_result": 20,
            "hash_index": 5,
            "status": "COMPLETE",
        }
    )

    results_insert = storage_socket.add_results([page1, page2, page3, page4, page5, page6])
    assert results_insert["meta"]["n_inserted"] == 6

    yield storage_socket


def test_server_log(storage_results):

    # Add something to double check the test
    mol_names = ["water_dimer_minima.psimol", "water_dimer_stretch.psimol", "water_dimer_stretch2.psimol"]

    molecules = [ptl.data.get_molecule(mol_name) for mol_name in mol_names]
    storage_results.molecule.add(molecules)

    storage_results.server_log.update_stats()
    _, ret = storage_results.server_log.query_stats(limit=1)
    assert ret[0]["db_table_size"] > 0
    assert ret[0]["db_total_size"] > 0

    for row in ret[0]["db_table_information"]["rows"]:
        if row[0] == "molecule":
            assert row[2] >= 1000

    # Check queries
    now = datetime.utcnow()
    meta, ret = storage_results.server_log.query_stats(after=now)
    assert meta.success
    assert meta.n_returned == 0
    assert meta.n_found == 0
    assert len(ret) == 0

    meta, ret = storage_results.server_log.query_stats(before=now)
    assert meta.success
    assert meta.n_returned > 0
    assert meta.n_found > 0
    assert len(ret) > 0

    # Make sure we are sorting correctly
    storage_results.server_log.update_stats()
    meta, ret = storage_results.server_log.query_stats(limit=1)
    assert meta.success
    assert meta.n_found > 1
    assert meta.n_returned == 1
    assert ret[0]["timestamp"] > now

    # Test get last stats
    ret2 = storage_results.server_log.get_latest_stats()
    assert ret[0] == ret2
