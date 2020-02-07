"""
Tests the on-node procedures compute capabilities.
"""

import copy

import numpy as np
import pytest
from qcelemental.util import msgpackext_dumps, msgpackext_loads

import qcfractal.interface as ptl
from qcfractal.interface.models import GridOptimizationInput, Molecule, TorsionDriveInput
from qcfractal.testing import fractal_compute_server, recursive_dict_merge, using_geometric, using_rdkit


@pytest.fixture(scope="module")
def torsiondrive_fixture(fractal_compute_server):

    # Cannot use this fixture without these services. Also cannot use `mark` and `fixture` decorators
    pytest.importorskip("torsiondrive")
    pytest.importorskip("geometric")
    pytest.importorskip("rdkit")

    client = ptl.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = ptl.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    # Geometric options
    torsiondrive_options = {
        "initial_molecule": mol_ret[0],
        "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [90]},
        "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
        "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
    }  # yapf: disable

    def spin_up_test(**keyword_augments):
        run_service = keyword_augments.pop("run_service", True)

        instance_options = copy.deepcopy(torsiondrive_options)
        recursive_dict_merge(instance_options, keyword_augments)

        inp = TorsionDriveInput(**instance_options)
        ret = client.add_service([inp], full_return=True)

        if ret.meta.n_inserted:  # In case test already submitted
            compute_key = ret.data.ids[0]
            service = client.query_services(procedure_id=compute_key)[0]
            assert "WAITING" in service["status"]

        if run_service:
            fractal_compute_server.await_services()
            assert len(fractal_compute_server.list_current_tasks()) == 0

        return ret.data

    ret = spin_up_test()

    torsion = fractal_compute_server.storage.get_procedures(id=ret.ids[0])["data"][0]

    yield torsion, client


def test_result_count_queries(torsiondrive_fixture, fractal_compute_server):

    torsion, client = torsiondrive_fixture

    r = fractal_compute_server.storage.custom_query("result", "count")["data"]
    assert isinstance(r, int)
    assert r > 10

    r = fractal_compute_server.storage.custom_query("result", "count", groupby=["result_type", "status"])["data"]
    assert isinstance(r, list)
    assert len(r) >= 3
    assert {"result_type", "status", "count"} == r[0].keys()
    assert r[0]["count"] > 0


def test_molecule_count_queries(torsiondrive_fixture, fractal_compute_server):

    torsion, client = torsiondrive_fixture

    r = fractal_compute_server.storage.custom_query("molecule", "count")["data"]
    assert isinstance(r, int)
    assert r > 10


def test_torsiondrive_initial_final_molecule(torsiondrive_fixture, fractal_compute_server):
    """ With single initial molecule in torsion proc"""

    torsion, client = torsiondrive_fixture

    r = fractal_compute_server.storage.custom_query("torsiondrive", "initial_molecules_ids", torsion_id=torsion["id"])

    assert r["meta"]["success"]
    assert len(r["data"]) == 9

    r = fractal_compute_server.storage.custom_query("torsiondrive", "initial_molecules", torsion_id=torsion["id"])
    assert r["meta"]["success"]
    assert len(r["data"]) == 9
    mol = r["data"][0]

    # Msgpack field
    assert isinstance(msgpackext_loads(mol["geometry"]), np.ndarray)  # TODO

    # Sample fields in the molecule dict
    assert all(x in mol.keys() for x in ["schema_name", "symbols", "geometry", "molecular_charge"])

    r = fractal_compute_server.storage.custom_query("torsiondrive", "final_molecules_ids", torsion_id=torsion["id"])

    assert r["meta"]["success"]
    assert len(r["data"]) == 9

    r = fractal_compute_server.storage.custom_query("torsiondrive", "final_molecules", torsion_id=torsion["id"])
    assert r["meta"]["success"]
    assert len(r["data"]) == 9
    mol = r["data"][0]

    # TODO: can't automatically convert msgpack
    # assert Molecule(**r['data'][0], validate=False, validated=True)


def test_torsiondrive_return_results(torsiondrive_fixture, fractal_compute_server):
    """ With single initial molecule in torsion proc"""

    torsion, client = torsiondrive_fixture

    r = fractal_compute_server.storage.custom_query("torsiondrive", "return_results", torsion_id=torsion["id"])
    assert r["meta"]["success"]
    assert len(r["data"])
    assert all(x in r["data"][0] for x in ["result_id", "return_result"])


def test_optimization_best_results(torsiondrive_fixture, fractal_compute_server):
    """ Test return best optimization proc results in one query"""

    torsion, client = torsiondrive_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = fractal_compute_server.storage.custom_query("optimization", "final_result", optimization_ids=opt_ids)

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))


def test_optimization_all_results(torsiondrive_fixture, fractal_compute_server):
    """ Test return best optimization proc results in one query"""

    torsion, client = torsiondrive_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = fractal_compute_server.storage.custom_query("optimization", "all_results", optimization_ids=opt_ids)

    # print('\ndata: \n--------\n', r['data'])

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))


def test_optimization_initial_molecules(torsiondrive_fixture, fractal_compute_server):
    """ Test return best optimization proc results in one query"""

    torsion, client = torsiondrive_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = fractal_compute_server.storage.custom_query("optimization", "initial_molecule", optimization_ids=opt_ids)

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))


def test_optimization_final_molecules(torsiondrive_fixture, fractal_compute_server):
    """ Test return best optimization proc results in one query"""

    torsion, client = torsiondrive_fixture

    opt_ids = [torsion["optimization_history"][k][v] for k, v in torsion["minimum_positions"].items()]
    opt_ids = set(opt_ids)

    r = fractal_compute_server.storage.custom_query("optimization", "final_molecule", optimization_ids=opt_ids)

    assert r["meta"]["success"]
    assert len(r["data"]) == len(opt_ids)
    assert set(r["data"].keys()) == set(map(int, opt_ids))
