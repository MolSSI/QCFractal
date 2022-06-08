from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.molecules import Molecule
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_manybody_full_1(fulltest_client: PortalClient):
    molecule = Molecule(
        symbols=["He", "He", "He", "He"],
        geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 3.77945225, 0.0, 3.77945225, 0.0, 0.0, 3.77945225, 3.77945225],
        fragments=[[0], [1], [2], [3]],
    )

    sp_spec = {
        "program": "psi4",
        "driver": "energy",
        "method": "mp2",
        "basis": "aug-cc-pvdz",
        "keywords": {"e_convergence": 1e-10, "d_convergence": 1e-10},
    }

    mb_keywords = {"max_nbody": None, "bsse_correction": "none"}

    meta, ids = fulltest_client.add_manybodys(
        initial_molecules=[molecule], program="manybody", singlepoint_specification=sp_spec, keywords=mb_keywords
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_manybodys(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete


def test_manybody_full_2(fulltest_client: PortalClient):
    molecule = Molecule(
        symbols=["He", "He", "He", "He"],
        geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 3.77945225, 0.0, 3.77945225, 0.0, 0.0, 3.77945225, 3.77945225],
        fragments=[[0], [1], [2], [3]],
    )

    sp_spec = {
        "program": "psi4",
        "driver": "energy",
        "method": "mp2",
        "basis": "aug-cc-pvdz",
        "keywords": {"e_convergence": 1e-10, "d_convergence": 1e-10},
    }

    mb_keywords = {"max_nbody": 2, "bsse_correction": "cp"}

    meta, ids = fulltest_client.add_manybodys(
        initial_molecules=[molecule], program="manybody", singlepoint_specification=sp_spec, keywords=mb_keywords
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_manybodys(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
