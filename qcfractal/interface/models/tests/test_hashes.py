import json

import pytest

from ..common_models import KeywordSet, Molecule
from ..proc_models import OptimizationModel
from ..gridoptimization import GridOptimizationInput
from ..torsiondrive import TorsionDrive

## Molecule hashes


def test_molecule_water_canary_hash():

    water_dimer_minima = Molecule.from_data(
        """
    0 1
    O  -1.551007  -0.114520   0.000000
    H  -1.934259   0.762503   0.000000
    H  -0.599677   0.040712   0.000000
    --
    O   1.350625   0.111469   0.000000
    H   1.680398  -0.373741  -0.758561
    H   1.680398  -0.373741   0.758561
    """,
        dtype="psi4")
    assert water_dimer_minima.get_hash() == "e816b396c7b00e49ef2d9c8b670c955df0a410c7"

    # Check orientation
    mol = water_dimer_minima.orient_molecule()
    assert mol.get_hash() == "b9bbe6028825d2e61c1ccfcdd0f4be4c3fa6efda"

    frag_0 = mol.get_fragment(0, orient=True)
    frag_1 = mol.get_fragment(1, orient=True)
    assert frag_0.get_hash() == "d8975ddd917a57f468596b54968b0dffe52c7487"
    assert frag_1.get_hash() == "feb5c6127ca54d715b999c15ea1ea1772ada8c5d"

@pytest.mark.parametrize("geom, hash_index", [
    ([0, 0, 0, 0, 0, 1], "6000063f9d7631a27e00a4b54d0b6b28a0a5b591"),
    ([0, 0, 0, 0, 0, 1 + 1.e-12], "6000063f9d7631a27e00a4b54d0b6b28a0a5b591"),
    ([0, 0, 0, 0, 0, 1 + 1.e-7], "7df4e4c420e2c5b3ef0f18b8e5b65c91e8370064"),
]) # yapf: disable
def test_molecule_geometry_canary_hash(geom, hash_index):

    mol = Molecule(geometry=geom, symbols=["H", "H"])

    assert mol.get_hash() == hash_index


## Keyword Set hash

@pytest.mark.parametrize("data, hash_index", [

    # Simple checks
    ({"values": {"hi": 5}},
     "53042da4ac1af059816008631e4589a0cd0c98c6"),

    ({"values": {"HI": 5}},
     "53042da4ac1af059816008631e4589a0cd0c98c6"),

    # Check options combinations
    ({"values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}},
     "888837dec981e2f9a1ef2ef2d33db6d239df65da"),

    ({"values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}, "lowercase": False},
     "08f6f37fec721b753096d71f7dbbccb734e8a696"),

    ({"values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}, "exact_floats": True},
     "691c86202af20868bba457514e63c83d0444142e"),

    ({"values": {"HI": [1, 2, 3], "All": [1.e-5, 2.e-3, 1.e-16]}, "exact_floats": True, "lowercase": False},
     "a3a6eea9edec3c2901a76fa8a41d4f9db9b3405f"),

    # Check recursive
    ({"values": {"d1": {"D2": [1.e-17, 5]}}},
     "55c6f1c7e610ce379feaa8a97854ea61974e0d92"),

    ({"values": {"d1": {"D2": [1.e-17, 5]}}, "exact_floats": True, "lowercase": False},
     "639d207f6a58ed2974f9737c3240ebffdf459857"),

    # Check hash_index build
    ({"values": {}, "hash_index": "waffles"},
     "waffles"),

    ({"values": {}, "hash_index": "waffles", "build_index": True},
     "bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"),
]) # yapf: disable
def test_keywords_canary_hash(data, hash_index):
    """
    Canary test aginst possible changes in the options hash_index
    """

    opt = KeywordSet(**data)

    assert hash_index == opt.hash_index, opt.values


@pytest.mark.parametrize("data1, data2", [
    # Test case sensitivity
    ({"values": {}},
     {"values": {}}),

    # Test float creation
    ({"values": {"CAPS": 5, "other": 4.e-3}},
     {"values": {"caps": 5, "other": 0.004}}),

    ({"values": {"other": 4.123e-5}},
     {"values": {"OTHER": 0.00004123}}),

    # Test list of floats
    ({"values": {"other": [1.11e-2, 2.22e-3]}},
     {"values": {"other": [0.0111, 0.00222]}}),

    # Test small floats
    ({"values": {"other": 1 + 1.e-17}},
     {"values": {"other": 1.0 - 1.e-17}}),

    # Test dict order
    ({"values": {"a": 5, "b": 6, "c": None}},
     {"values": {"b": 6, "a": 5, "c": None}}),

    # Check recusive
    ({"values": {"d1": {"D2": [0.0, 5], "d3": (3, 1.e-17)}}},
     {"values": {"d1": {"d2": [1.e-17, 5], "d3": (3, 0)}}}),

]) # yapf: disable
def test_keywords_comparison_hash(data1, data2):
    """
    Ensure the hash_index finds collisions correctly before and after serialization.
    """
    opt1 = KeywordSet(**data1)
    opt2 = KeywordSet(**data2)

    # Check after serialization and rebuild index
    opt1s = KeywordSet(**json.loads(opt1.json()), build_index=True)
    opt2s = KeywordSet(**json.loads(opt2.json()), build_index=True)

    # Paranoid, try all combinations
    assert opt1.hash_index == opt2.hash_index
    assert opt1.hash_index == opt2s.hash_index
    assert opt1s.hash_index == opt2.hash_index
    assert opt1s.hash_index == opt2s.hash_index


## Optimization hashes
_qc_spec = {"driver": "gradient", "method": "HF", "basis": "sto-3g", "keywords": None, "program": "prog"}
_base_opt = {
    "keywords": {},
    "program": "prog2",
    "initial_molecule": "5c7896fb95d592ad07a2fe3b",
    "success": False,
    "qc_spec": _qc_spec
}
@pytest.mark.parametrize("data, hash_index", [

    # Check same
    ({},
     "254de59f1598570d0c31aa2d3d84b601c9da12b9"),

    ({"program": "PROG2"},
     "254de59f1598570d0c31aa2d3d84b601c9da12b9"),

    ({"qc_spec": {**_qc_spec, **{"program": "prog"}}},
     "254de59f1598570d0c31aa2d3d84b601c9da12b9"),

    ({"qc_spec": {**_qc_spec, **{"method": "HF"}}},
     "254de59f1598570d0c31aa2d3d84b601c9da12b9"),

    # Check tolerances
    ({"keywords": {"tol": 1.e-12}},
     "8ab52bff9430f7759323e6a547afc58725422c47"),

    ({"keywords": {"tol": 0.0}},
     "8ab52bff9430f7759323e6a547afc58725422c47"),

    ({"keywords": {"tol": 1.e-9}},
     "1628caf9a29c9bf17a66cb55b13106e7f2704e51"),

    # Check fields
    ({"initial_molecule": "5c78987e95d592ad07a2fe3c"},
     "3c20c8f7b1be857460f2d71d74680dd19e9d9113"),

    # Check basis preps
    ({"qc_spec": {**_qc_spec, **{"basis": None}}},
     "3489e0c47144ebedb4fdcc2bfab61f7aa4dc947c"),

    ({"qc_spec": {**_qc_spec, **{"basis": ""}}},
     "3489e0c47144ebedb4fdcc2bfab61f7aa4dc947c"),
]) # yapf: disable

def test_optimization_canary_hash(data, hash_index):

    opt = OptimizationModel(**{**_base_opt, **data})

    assert hash_index == opt.hash_index, data


## GridOptimization hashes
_opt_spec = {
    "program": "geometric",
    "keywords": {
        "coordsys": "tric",
    }
}

_scan_spec = {
        "type": "distance",
        "indices": [1, 2],
        "steps": [-0.1, 0.0],
        "step_type": "relative"
    } # yapf: disable

_base_gridopt = {
    "keywords": {
        "preoptimization": False,
        "scans": [_scan_spec]
    },
    "optimization_spec": _opt_spec,
    "qc_spec": _qc_spec,
    "initial_molecule": "5c7896fb95d592ad07a2fe3b",
}
@pytest.mark.parametrize("data, hash_index", [

    # Check same
    ({},
     "6bf2bce9b49cf669fe01d064321ecdd42ff59d5f"),

    ({"keywords": {
        "preoptimization": False,
        "scans": [{**_scan_spec, **{"steps": [-0.1 + 1e-12, 0.0 - 1.e-12]}}]
    }},
     "6bf2bce9b49cf669fe01d064321ecdd42ff59d5f"),


    # Check opt keywords stability
    ({"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 1.e-12}}}},
     "6bf2bce9b49cf669fe01d064321ecdd42ff59d5f"),

    ({"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 0}}}},
     "6bf2bce9b49cf669fe01d064321ecdd42ff59d5f"),

    # Check fields
    ({"initial_molecule": "5c78987e95d592ad07a2fe3c"},
     "5b00f25ce8a81950754faf65b1643896837ea0ec"),

])
def test_gridoptimization_canary_hash(data, hash_index):

    gridopt = GridOptimizationInput(**{**_base_gridopt, **data})

    assert hash_index == gridopt.get_hash_index(), data

## TorsionDrive hashes


_base_torsion = {
    "keywords": {
        "dihedrals": [[0, 1, 2, 3]],
        "grid_spacing": [10]
    },
    "optimization_spec": _opt_spec,
    "qc_spec": _qc_spec,
    "initial_molecule": ["5c7896fb95d592ad07a2fe3b"],
    "final_energy_dict": {},
    "optimization_history": {},
    "minimum_positions": {},
    "provenance": {"creator": ""}
}

@pytest.mark.parametrize("data, hash_index", [

    # Check same
    ({},
     "539022b987b84a8888a88789224c42096f11f5fc"),

    ({"keywords": {
        "dihedrals": [[0, 1, 2, 3]],
        "grid_spacing": [10],
        "tol": 1.e-12
    }},
     "972c731248b800a4e8984820333ed2b0fd3ac372"),

    ({"keywords": {
        "dihedrals": [[0, 1, 2, 3]],
        "grid_spacing": [10],
        "tol": 0
    }},
     "972c731248b800a4e8984820333ed2b0fd3ac372"),

    ({"keywords": {
        "dihedrals": [[0, 1, 2, 3]],
        "grid_spacing": [10],
        "tol": 1.e-9
    }},
     "f0d09cb058501e18001c7e454dafe42944d5f45e"),

    # Check opt keywords stability
    ({"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 1.e-12}}}},
     "c4cf09b80f6cb77bb3d5f41a3888d7b877205ef4"),

    ({"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 0}}}},
     "c4cf09b80f6cb77bb3d5f41a3888d7b877205ef4"),

    # Check fields
    ({"initial_molecule": ["5c78987e95d592ad07a2fe3c"]},
     "e37272983b3c2f6dcca74bb45f823f33d0cb3b11"),

])
def test_torsiondrive_canary_hash(data, hash_index):

    td = TorsionDrive(**{**_base_torsion, **data})

    assert hash_index == td.get_hash_index(), data