from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcelemental.models import Molecule

from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_singlepoint_full_1(fulltest_client: PortalClient):
    mol = Molecule(symbols=["H", "H"], geometry=[[0.0, 0.0, 0.0], [0.0, 0.0, 1.0]])
    meta, ids = fulltest_client.add_singlepoints(
        mol,
        "psi4",
        "energy",
        "hf",
        "sto-3g",
    )

    for i in range(120):
        time.sleep(1)
        rec = fulltest_client.get_singlepoints(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete


def test_singlepoint_full_2(fulltest_client: PortalClient):
    mol = Molecule(
        symbols=["H", "H", "O"],
        geometry=[[1.0, 1.0, 0.0], [-1.0, 1.0, 0.0], [0.0, 0.0, 0.0]],
        connectivity=[[0, 2, 1], [1, 2, 1]],
    )
    meta, ids = fulltest_client.add_singlepoints(
        mol,
        "rdkit",
        "energy",
        "uff",
        "",
    )

    for i in range(120):
        time.sleep(1)
        rec = fulltest_client.get_singlepoints(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete


def test_singlepoint_full_error_1(fulltest_client: PortalClient):
    # sto-3g not defined for U
    mol = Molecule(symbols=["U"], geometry=[[0.0, 0.0, 0.0]])

    meta, ids = fulltest_client.add_singlepoints(
        mol,
        "psi4",
        "energy",
        "b3lyp",
        "sto-3g",
    )

    for i in range(120):
        time.sleep(1)
        rec = fulltest_client.get_singlepoints(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.error
    assert "Unable to find a basis set" in rec.error["error_message"]


def test_singlepoint_full_error_2(fulltest_client: PortalClient):
    # rdkit requires connectivity
    mol = Molecule(symbols=["H", "H", "O"], geometry=[[1.0, 1.0, 0.0], [-1.0, 1.0, 0.0], [0.0, 0.0, 0.0]])
    meta, ids = fulltest_client.add_singlepoints(
        mol,
        "rdkit",
        "energy",
        "uff",
        "",
    )

    for i in range(120):
        time.sleep(1)
        rec = fulltest_client.get_singlepoints(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.error
    assert "RDKit requires molecules to have a connectivity graph" in rec.error["error_message"]
