import json

import pytest

from ..common_models import Molecule
from qcfractal.portal.components.keywords.models import KeywordSet
from ..gridoptimization import GridOptimizationRecord
from ..records import OptimizationRecord, ResultRecord
from ..torsiondrive import TorsionDriveRecord

## Molecule hashes


## Keyword Set hash


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
