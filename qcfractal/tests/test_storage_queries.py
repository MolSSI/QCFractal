"""
Tests the on-node procedures compute capabilities.
"""

import copy

import numpy as np
import pytest
from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from qcelemental.util import msgpackext_dumps, msgpackext_loads

from qcfractal.testing import run_services


@pytest.fixture(scope="function")
def storage_query_fixture(torsiondrive_fixture):
    # Takes the default torsiondrive fixture and runs it

    spin_up_test, server, _ = torsiondrive_fixture
    ret = spin_up_test()
    torsion_id = ret.ids[0]

    storage_socket = SQLAlchemySocket(server._qcf_config)

    torsion = storage_socket.procedure.get(id=[torsion_id])["data"][0]

    yield storage_socket, torsion


def test_result_count_queries(storage_query_fixture):
    storage_socket, _ = storage_query_fixture

    r = storage_socket.custom_query("result", "count")["data"]
    assert isinstance(r, int)
    assert r > 10

    r = storage_socket.custom_query("result", "count", groupby=["result_type", "status"])["data"]
    assert isinstance(r, list)
    assert len(r) >= 3
    assert {"result_type", "status", "count"} == r[0].keys()
    assert r[0]["count"] > 0


def test_molecule_count_queries(storage_query_fixture):
    storage_socket, _ = storage_query_fixture

    r = storage_socket.custom_query("molecule", "count")["data"]
    assert isinstance(r, int)
    assert r >= 10


def test_torsiondrive_initial_final_molecule(storage_query_fixture):
    """With single initial molecule in torsion proc"""

    storage_socket, torsion = storage_query_fixture

    r = storage_socket.custom_query("torsiondrive", "initial_molecules_ids", torsion_id=torsion["id"])

    assert r["meta"]["success"]
    assert len(r["data"]) == 9

    r = storage_socket.custom_query("torsiondrive", "initial_molecules", torsion_id=torsion["id"])
    assert r["meta"]["success"]
    assert len(r["data"]) == 9
    mol = r["data"][0]

    # Msgpack field
    assert isinstance(msgpackext_loads(mol["geometry"]), np.ndarray)  # TODO

    # Sample fields in the molecule dict
    assert all(x in mol.keys() for x in ["schema_name", "symbols", "geometry", "molecular_charge"])

    r = storage_socket.custom_query("torsiondrive", "final_molecules_ids", torsion_id=torsion["id"])

    assert r["meta"]["success"]
    assert len(r["data"]) == 9

    r = storage_socket.custom_query("torsiondrive", "final_molecules", torsion_id=torsion["id"])
    assert r["meta"]["success"]
    assert len(r["data"]) == 9
    mol = r["data"][0]

    # TODO: can't automatically convert msgpack
    # assert Molecule(**r['data'][0], validate=False, validated=True)


def test_torsiondrive_return_results(storage_query_fixture):
    """With single initial molecule in torsion proc"""

    storage_socket, torsion = storage_query_fixture

    r = storage_socket.custom_query("torsiondrive", "return_results", torsion_id=torsion["id"])
    assert r["meta"]["success"]
    assert len(r["data"])
    assert all(x in r["data"][0] for x in ["result_id", "return_result"])


def test_optimization_best_results(storage_query_fixture):
    """Test return best optimization proc results in one query"""

    storage_socket, torsion = storage_query_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = storage_socket.custom_query("optimization", "final_result", optimization_ids=opt_ids)

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))


def test_optimization_all_results(storage_query_fixture):
    """Test return best optimization proc results in one query"""

    storage_socket, torsion = storage_query_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = storage_socket.custom_query("optimization", "all_results", optimization_ids=opt_ids)

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))


def test_optimization_initial_molecules(storage_query_fixture):
    """Test return best optimization proc results in one query"""

    storage_socket, torsion = storage_query_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = storage_socket.custom_query("optimization", "initial_molecule", optimization_ids=opt_ids)

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))


def test_optimization_final_molecules(storage_query_fixture):
    """Test return best optimization proc results in one query"""

    storage_socket, torsion = storage_query_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = storage_socket.custom_query("optimization", "final_molecule", optimization_ids=opt_ids)

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))
