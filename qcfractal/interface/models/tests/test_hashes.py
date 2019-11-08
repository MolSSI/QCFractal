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


## Record hashes
_base_result = {
    "driver": "gradient",
    "method": "hf",
    "basis": "sto-3g",
    "keywords": None,
    "program": "prog",
    "molecule": "5c7896fb95d592ad07a2fe3b",
}


@pytest.mark.parametrize(
    "data, hash_index",
    [
        # Check same
        ({}, "e1e20d5c13c8ad7ba894f71af39f3c0884ef2aca"),
        ({"program": "PROG"}, "e1e20d5c13c8ad7ba894f71af39f3c0884ef2aca"),
        ({"method": "HF"}, "e1e20d5c13c8ad7ba894f71af39f3c0884ef2aca"),
        # All different
        ({"program": "prog2"}, "f8638510976e5146b6bef79995f6f14acd06f645"),
        ({"driver": "energy"}, "4c2c737694f9438c73b294d7f72249acbbd3b649"),
        ({"keywords": "5c7896fb95d592ad07a2fe3b"}, "32e115a991ffb85ac0c29bf4dd5d93e733919b5c"),
        # Check same
        ({"basis": ""}, "1ae5df953fc38f9b866bd50af6d5513dbfc49cc6"),
        ({"basis": "null"}, "1ae5df953fc38f9b866bd50af6d5513dbfc49cc6"),
        ({"basis": None}, "1ae5df953fc38f9b866bd50af6d5513dbfc49cc6"),
    ],
)  # yapf: disable
def test_result_record_canary_hash(data, hash_index):

    opt = ResultRecord(**{**_base_result, **data})

    assert hash_index == opt.get_hash_index(), data
    assert opt.hash_index is None  # Not set


## Optimization hashes
_qc_spec = {"driver": "gradient", "method": "HF", "basis": "sto-3g", "keywords": None, "program": "prog"}
_base_opt = {"keywords": {}, "program": "prog2", "initial_molecule": "5c7896fb95d592ad07a2fe3b", "qc_spec": _qc_spec}


@pytest.mark.parametrize(
    "data, hash_index",
    [
        # Check same
        ({}, "254de59f1598570d0c31aa2d3d84b601c9da12b9"),
        ({"program": "PROG2"}, "254de59f1598570d0c31aa2d3d84b601c9da12b9"),
        ({"qc_spec": {**_qc_spec, **{"program": "prog"}}}, "254de59f1598570d0c31aa2d3d84b601c9da12b9"),
        ({"qc_spec": {**_qc_spec, **{"method": "HF"}}}, "254de59f1598570d0c31aa2d3d84b601c9da12b9"),
        # Check tolerances
        ({"keywords": {"tol": 1.0e-12}}, "8ab52bff9430f7759323e6a547afc58725422c47"),
        ({"keywords": {"tol": 0.0}}, "8ab52bff9430f7759323e6a547afc58725422c47"),
        ({"keywords": {"tol": 1.0e-9}}, "1628caf9a29c9bf17a66cb55b13106e7f2704e51"),  # Should be different from above
        # Check fields
        ({"initial_molecule": "5c78987e95d592ad07a2fe3c"}, "3c20c8f7b1be857460f2d71d74680dd19e9d9113"),
        # Check basis preps
        ({"qc_spec": {**_qc_spec, **{"basis": None}}}, "3489e0c47144ebedb4fdcc2bfab61f7aa4dc947c"),
        ({"qc_spec": {**_qc_spec, **{"basis": ""}}}, "3489e0c47144ebedb4fdcc2bfab61f7aa4dc947c"),
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
    "initial_molecule": "5c7896fb95d592ad07a2fe3b",
    "starting_molecule": "5c7896fb95d592ad07a2fe3b",
    "grid_optimizations": {},
    "final_energy_dict": {},
    "starting_grid": tuple(),
    "provenance": {"creator": ""},
}


@pytest.mark.parametrize(
    "data, hash_index",
    [
        # Check same
        ({}, "fa2da83aae1651a9115f5eaea83043187c4c8c7b"),
        (
            {
                "keywords": {
                    "preoptimization": False,
                    "scans": [{**_scan_spec, **{"steps": [-0.1 + 1e-12, 0.0 - 1.0e-12]}}],
                }
            },
            "fa2da83aae1651a9115f5eaea83043187c4c8c7b",
        ),
        # Check opt keywords stability
        (
            {"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 1.0e-12}}}},
            "fa2da83aae1651a9115f5eaea83043187c4c8c7b",
        ),
        ({"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 0}}}}, "fa2da83aae1651a9115f5eaea83043187c4c8c7b"),
        # Check fields
        ({"initial_molecule": "5c78987e95d592ad07a2fe3c"}, "9624ce2eca96eabcdb9ec3b2e073429f6dd4b8a4"),
        ({"qc_spec": {**_qc_spec, **{"method": "custom1"}}}, "d6a187bf6de7a25d36402c5d16109ddc6f4f217d"),
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
    "initial_molecule": ["5c7896fb95d592ad07a2fe3b"],
    "final_energy_dict": {},
    "optimization_history": {},
    "minimum_positions": {},
    "provenance": {"creator": ""},
}


@pytest.mark.parametrize(
    "data, hash_index",
    [
        ({}, "dd305011ee2b741b1dcd03350994920a3718b289"),
        # Check same
        (
            {"keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10], "energy_upper_limit": 1.0e-12}},
            "37b65cba19ec4fbd0d54c10fd74d0a27f627cdea",
        ),
        (
            {"keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10], "energy_upper_limit": 0}},
            "37b65cba19ec4fbd0d54c10fd74d0a27f627cdea",
        ),
        (
            {"keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [10], "energy_upper_limit": 1.0e-9}},
            "64b400229d3e5bff476e47c093c1a159c69d9fdc",
        ),
        # Check opt keywords stability
        (
            {"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 0.0}}}},
            "a12fd524b0e215b5252b464ca4041091916df8bb",
        ),
        (
            {"optimization_spec": {**_opt_spec, **{"keywords": {"tol": 1.0e-12}}}},
            "a12fd524b0e215b5252b464ca4041091916df8bb",
        ),
        # Check fields
        ({"initial_molecule": ["5c78987e95d592ad07a2fe3c"]}, "f209751a4a6559a8d2d539c070f3b701d1ddf9f2"),
    ],
)  # yapf disable
def test_torsiondrive_canary_hash(data, hash_index):

    td = TorsionDriveRecord(**{**_base_torsion, **data})

    assert hash_index == td.get_hash_index(), data
