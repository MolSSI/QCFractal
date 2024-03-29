from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_torsiondrive_full_1(fulltest_client: PortalClient):
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

    td_keywords = {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [90]}

    opt_spec = {
        "keywords": {},
        "program": "geometric",
        "protocols": {"trajectory": "none"},
        "qc_specification": {"basis": "sto-3g", "keywords": {}, "method": "pbe", "program": "psi4"},
    }

    meta, ids = fulltest_client.add_torsiondrives(
        initial_molecules=[[molecule]],
        program="torsiondrive",
        optimization_specification=opt_spec,
        keywords=td_keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_torsiondrives(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
