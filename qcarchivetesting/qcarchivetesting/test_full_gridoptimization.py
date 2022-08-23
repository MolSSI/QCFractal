from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_gridoptimization_full_1(fulltest_client: PortalClient):
    molecule = Molecule(
        symbols=["H", "O", "O", "H"],
        connectivity=[[0, 1, 1], [1, 2, 1], [2, 3, 1]],
        geometry=[
            1.848671612718783,
            1.4723466699847623,
            0.6446435664312682,
            1.3127881568370925,
            -0.1304193792618355,
            -0.2118922703584585,
            -1.3127927010942337,
            0.1334187339129038,
            -0.21189641512867613,
            -1.8386801669381663,
            -1.482348324549995,
            0.6446369709610646,
        ],
    )

    opt_spec = {
        "keywords": {},
        "program": "geometric",
        "protocols": {"trajectory": "none"},
        "qc_specification": {"basis": "sto-3g", "keywords": {}, "method": "blyp", "program": "psi4"},
    }

    go_keywords = {
        "preoptimization": False,
        "scans": [
            {"indices": [1, 2], "step_type": "relative", "steps": [-0.1, 0.0], "type": "distance"},
            {"indices": [0, 1, 2, 3], "step_type": "absolute", "steps": [-90, 0], "type": "dihedral"},
        ],
    }

    meta, ids = fulltest_client.add_gridoptimizations(
        initial_molecules=[molecule],
        program="gridoptimization",
        optimization_specification=opt_spec,
        keywords=go_keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_gridoptimizations(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete


def test_gridoptimization_full_2(fulltest_client: PortalClient):
    molecule = Molecule(
        symbols=["H", "O", "O", "H"],
        connectivity=[[0, 1, 1], [1, 2, 1], [2, 3, 1]],
        geometry=[
            1.848671612718783,
            1.4723466699847623,
            0.6446435664312682,
            1.3127881568370925,
            -0.1304193792618355,
            -0.2118922703584585,
            -1.3127927010942337,
            0.1334187339129038,
            -0.21189641512867613,
            -1.8386801669381663,
            -1.482348324549995,
            0.6446369709610646,
        ],
    )

    input_opt_spec = {
        "keywords": {},
        "program": "geometric",
        "protocols": {"trajectory": "none"},
        "qc_specification": {"basis": "sto-3g", "keywords": {}, "method": "blyp", "program": "psi4"},
    }

    go_keywords = {
        "preoptimization": True,
        "scans": [
            {"indices": [1, 2], "step_type": "relative", "steps": [-0.1, 0.0], "type": "distance"},
            {"indices": [0, 1, 2, 3], "step_type": "absolute", "steps": [-90, 0], "type": "dihedral"},
        ],
    }

    meta, ids = fulltest_client.add_gridoptimizations(
        initial_molecules=[molecule],
        program="gridoptimization",
        optimization_specification=input_opt_spec,
        keywords=go_keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_gridoptimizations(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
