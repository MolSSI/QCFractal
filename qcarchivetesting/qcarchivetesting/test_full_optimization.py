from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcelemental.models import Molecule

from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_optimization_full_1(fulltest_client: PortalClient):
    mol = Molecule(symbols=["H", "H"], geometry=[[0.0, 0.0, 0.0], [0.0, 0.0, 1.0]])
    meta, ids = fulltest_client.add_optimizations(
        initial_molecules=[mol],
        program="geometric",
        qc_specification={
            "program": "psi4",
            "method": "b3lyp",
            "basis": "6-31g**",
            "keywords": {"maxiter": 100},
        },
        keywords={"maxiter": 30},
    )

    for i in range(120):
        time.sleep(1)
        rec = fulltest_client.get_optimizations(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete


def test_optimization_full_error_1(fulltest_client: PortalClient):
    mol = Molecule(symbols=["H", "H"], geometry=[[0.0, 0.0, 0.0], [0.0, 0.0, 1.0]])
    meta, ids = fulltest_client.add_optimizations(
        initial_molecules=[mol],
        program="geometric",
        qc_specification={
            "program": "psi4",
            "method": "b3lyp",
            "basis": "6-31g**",
            "keywords": {"maxiter": 1},
        },
        keywords={"maxiter": 30},
    )

    for i in range(120):
        time.sleep(1)
        rec = fulltest_client.get_optimizations(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.error
