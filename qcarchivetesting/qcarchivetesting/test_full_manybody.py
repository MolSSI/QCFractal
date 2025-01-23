from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum

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

    levels = {1: sp_spec, 2: sp_spec, 3: sp_spec, 4: sp_spec}

    meta, ids = fulltest_client.add_manybodys(
        initial_molecules=[molecule],
        program="qcmanybody",
        bsse_correction=["nocp"],
        levels=levels,
        keywords={"return_total_data": True},
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

    sp_spec_1 = {
        "program": "psi4",
        "driver": "energy",
        "method": "mp2",
        "basis": "sto-3g",
        "keywords": {"cc_type": "df", "df_basis_mp2": "def2-qzvpp-ri"},
    }

    sp_spec_2 = {
        "program": "psi4",
        "driver": "energy",
        "method": "b3lyp",
        "basis": "sto-3g",
        "keywords": {"cc_type": "df", "df_basis_mp2": "def2-qzvpp-ri"},
    }

    sp_spec_3 = {
        "program": "psi4",
        "driver": "energy",
        "method": "hf",
        "basis": "sto-3g",
        "keywords": {"cc_type": "df", "df_basis_mp2": "def2-qzvpp-ri"},
    }

    levels = {
        1: sp_spec_1,
        2: sp_spec_2,
        "supersystem": sp_spec_3,
    }

    meta, ids = fulltest_client.add_manybodys(
        initial_molecules=[molecule],
        program="qcmanybody",
        bsse_correction=["nocp", "cp", "vmfc"],
        levels=levels,
        keywords={"return_total_data": True},
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_manybodys(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
