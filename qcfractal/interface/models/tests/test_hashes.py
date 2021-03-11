import json

import pytest

from ..common_models import KeywordSet, Molecule
from ..gridoptimization import GridOptimizationRecord
from ..records import OptimizationRecord, ResultRecord
from ..torsiondrive import TorsionDriveRecord

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
        dtype="psi4",
    )
    assert water_dimer_minima.get_hash() == "42f3ac52af52cf2105c252031334a2ad92aa911c"

    # Check orientation
    mol = water_dimer_minima.orient_molecule()
    assert mol.get_hash() == "632490a0601500bfc677e9277275f82fbc45affe"

    frag_0 = mol.get_fragment(0, orient=True)
    frag_1 = mol.get_fragment(1, orient=True)
    assert frag_0.get_hash() == "d0b499739f763e8d3a5556b4ddaeded6a148e4d5"
    assert frag_1.get_hash() == "bdc1f75bd1b7b999ff24783d7c1673452b91beb9"


@pytest.mark.parametrize(
    "geom",
    [[0, 0, 0, 0, 5, 0], [0, 0, 0, 0, 5, 0 + 1.0e-12], [0, 0, 0, 0, 5, 0 - 1.0e-12], [0, 0, 0, 0, 5, 0 + 1.0e-7]],
)  # yapf: disable
def test_molecule_geometry_canary_hash(geom):

    mol = Molecule(geometry=geom, symbols=["H", "H"])

    assert mol.get_hash() == "fb69e6744407b220a96d6ddab4ec2099619db791"


## Keyword Set hash


@pytest.mark.parametrize(
    "data, hash_index",
    [
        # Simple checks
        ({"values": {"hi": 5}}, "53042da4ac1af059816008631e4589a0cd0c98c6"),
        ({"values": {"HI": 5}}, "53042da4ac1af059816008631e4589a0cd0c98c6"),
        # Check options combinations
        ({"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}}, "888837dec981e2f9a1ef2ef2d33db6d239df65da"),
        (
            {"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}, "lowercase": False},
            "08f6f37fec721b753096d71f7dbbccb734e8a696",
        ),
        (
            {"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}, "exact_floats": True},
            "691c86202af20868bba457514e63c83d0444142e",
        ),
        (
            {"values": {"HI": [1, 2, 3], "All": [1.0e-5, 2.0e-3, 1.0e-16]}, "exact_floats": True, "lowercase": False},
            "a3a6eea9edec3c2901a76fa8a41d4f9db9b3405f",
        ),
        # Check recursive
        ({"values": {"d1": {"D2": [1.0e-17, 5]}}}, "55c6f1c7e610ce379feaa8a97854ea61974e0d92"),
        (
            {"values": {"d1": {"D2": [1.0e-17, 5]}}, "exact_floats": True, "lowercase": False},
            "639d207f6a58ed2974f9737c3240ebffdf459857",
        ),
        # Check hash_index build
        ({"values": {}, "hash_index": "waffles"}, "waffles"),
        ({"values": {}, "hash_index": "waffles", "build_index": True}, "bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f"),
    ],
)  # yapf: disable
def test_keywords_canary_hash(data, hash_index):
    """
    Canary test aginst possible changes in the options hash_index
    """

    opt = KeywordSet(**data)

    assert hash_index == opt.hash_index, opt.values


@pytest.mark.parametrize(
    "data1, data2",
    [
        # Test case sensitivity
        ({"values": {}}, {"values": {}}),
        # Test float creation
        ({"values": {"CAPS": 5, "other": 4.0e-3}}, {"values": {"caps": 5, "other": 0.004}}),
        ({"values": {"other": 4.123e-5}}, {"values": {"OTHER": 0.00004123}}),
        # Test list of floats
        ({"values": {"other": [1.11e-2, 2.22e-3]}}, {"values": {"other": [0.0111, 0.00222]}}),
        # Test small floats
        ({"values": {"other": 1 + 1.0e-17}}, {"values": {"other": 1.0 - 1.0e-17}}),
        # Test dict order
        ({"values": {"a": 5, "b": 6, "c": None}}, {"values": {"b": 6, "a": 5, "c": None}}),
        # Check recusive
        (
            {"values": {"d1": {"D2": [0.0, 5], "d3": (3, 1.0e-17)}}},
            {"values": {"d1": {"d2": [1.0e-17, 5], "d3": (3, 0)}}},
        ),
    ],
)  # yapf: disable
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


@pytest.mark.parametrize("model", [ResultRecord, OptimizationRecord])
def test_hash_fields(model):
    assert "procedure" in model.get_hash_fields()
    assert "program" in model.get_hash_fields()


## Optimization hashes
_qc_spec = {"driver": "gradient", "method": "HF", "basis": "sto-3g", "keywords": None, "program": "prog"}
_base_opt = {"keywords": {}, "program": "prog2", "initial_molecule": "76634", "qc_spec": _qc_spec}


@pytest.mark.parametrize(
    "data, hash_index",
    [
        # Check same
        ({}, "3f2ae89f38a712e701a64bb51a9c5e1638e295f7"),
        ({"program": "PROG2"}, "3f2ae89f38a712e701a64bb51a9c5e1638e295f7"),
        ({"qc_spec": {**_qc_spec, **{"program": "prog"}}}, "3f2ae89f38a712e701a64bb51a9c5e1638e295f7"),
        ({"qc_spec": {**_qc_spec, **{"method": "HF"}}}, "3f2ae89f38a712e701a64bb51a9c5e1638e295f7"),
        # Check tolerances
        ({"keywords": {"tol": 1.0e-12}}, "005fe9fd7a47a17a108256f90bf7fa1107d352f2"),
        ({"keywords": {"tol": 0.0}}, "005fe9fd7a47a17a108256f90bf7fa1107d352f2"),
        ({"keywords": {"tol": 1.0e-9}}, "29a510ac629ee8a4e483cbffb428d3369fe10de5"),  # Should be different from above
        # Check fields
        ({"initial_molecule": "84202"}, "9e32d55f58f39203d29db801172714d51c8d0eeb"),
        # Check basis preps
        ({"qc_spec": {**_qc_spec, **{"basis": None}}}, "55c23e37a1db6ae75f9c01b74daec171c5aed54f"),
        ({"qc_spec": {**_qc_spec, **{"basis": ""}}}, "55c23e37a1db6ae75f9c01b74daec171c5aed54f"),
    ],
)  # yapf: disable
def test_optimization_record_canary_hash(data, hash_index):

    opt = OptimizationRecord(**{**_base_opt, **data})

    assert hash_index == opt.hash_index, data


## GridOptimizationRecord hashes
_opt_spec = {"program": "geometric", "keywords": {"coordsys": "tric"}}

_scan_spec = {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"}  # yapf: disable

_base_gridopt = {
    "keywords": {"preoptimization": False, "scans": [_scan_spec]},
    "optimization_spec": _opt_spec,
    "qc_spec": _qc_spec,
    "initial_molecule": "12886",
    "starting_molecule": "12886",
    "grid_optimizations": {},
    "final_energy_dict": {},
    "starting_grid": tuple(),
    "provenance": {"creator": ""},
}


@pytest.mark.parametrize(
    "data, hash_index",
    [
        # Check same
        ({}, "7a6ea87f85e3cb21539592479b8b8e03a021d6ea"),
        (
            {
                "keywords": {
                    "preoptimization": False,
                    "scans": [{**_scan_spec, **{"steps": [-0.1 + 1e-12, 0.0 - 1.0e-12]}}],
                }
            },
            "7a6ea87f85e3cb21539592479b8b8e03a021d6ea",
        ),
        # Check opt keywords stability
        (
            {"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 1.0e-12}}}},
            "7a6ea87f85e3cb21539592479b8b8e03a021d6ea",
        ),
        ({"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 0}}}}, "7a6ea87f85e3cb21539592479b8b8e03a021d6ea"),
        # Check fields
        ({"initial_molecule": "78231"}, "ef670e2fe96081de257681fa7fe71a6df0c07447"),
        ({"qc_spec": {**_qc_spec, **{"method": "custom1"}}}, "64847da809f22adc272564c7b2280682c4f4b918"),
    ],
)  # yapf: disable
def test_gridoptimization_canary_hash(data, hash_index):

    gridopt = GridOptimizationRecord(**{**_base_gridopt, **data})

    assert hash_index == gridopt.get_hash_index(), data


## TorsionDriveRecord hashes

_base_torsion = {
    "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10]},
    "optimization_spec": _opt_spec,
    "qc_spec": _qc_spec,
    "initial_molecule": ["89712"],
    "final_energy_dict": {},
    "optimization_history": {},
    "minimum_positions": {},
    "provenance": {"creator": ""},
}


@pytest.mark.parametrize(
    "data, hash_index",
    [
        ({}, "67f61724520622e0c3376acc5c57defd7afdb970"),
        # Check same
        (
            {"keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10], "energy_upper_limit": 1.0e-12}},
            "3f8637e2873ee9424e13a0a9aa485e594f8bd561",
        ),
        (
            {"keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10], "energy_upper_limit": 0}},
            "3f8637e2873ee9424e13a0a9aa485e594f8bd561",
        ),
        (
            {"keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10], "energy_upper_limit": 1.0e-9}},
            "1e5f5c2d4216ab11e458766124f94f32c0697f0a",
        ),
        # Check opt keywords stability
        (
            {"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 0.0}}}},
            "75c1506ae205f2978c1dcc3c9b25d2013e28e69b",
        ),
        (
            {"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 1.0e-12}}}},
            "75c1506ae205f2978c1dcc3c9b25d2013e28e69b",
        ),
        # Check fields
        ({"initial_molecule": ["22718"]}, "d9f703e2cb11a4d7dc5aa3e0c630625d057864ae"),
    ],
)  # yapf disable
def test_torsiondrive_canary_hash(data, hash_index):

    td = TorsionDriveRecord(**{**_base_torsion, **data})

    assert hash_index == td.get_hash_index(), data
