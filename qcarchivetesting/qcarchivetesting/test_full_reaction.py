from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient

molecule_H2 = Molecule(
    symbols=["H", "H"],
    geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 1.401],
)

molecule_O2 = Molecule(symbols=["O", "O"], geometry=[0.0, 0.0, 0.0, 0.0, 0.0, 2.2828])

molecule_H2O = Molecule(
    symbols=["O", "H", "H"],
    geometry=[0.0, 0.0, 0.2217, -4.380867762249869e-17, 1.4309, -0.8867, 4.380867762249869e-17, -1.4309, -0.8867],
)


def test_reaction_full_1(fulltest_client: PortalClient):
    sp_spec = {"program": "psi4", "driver": "energy", "method": "b3lyp", "basis": "def2-tzvp", "keywords": {}}

    rxn_keywords = {}

    meta, ids = fulltest_client.add_reactions(
        stoichiometries=[[(-2.0, molecule_H2), (-1.0, molecule_O2), (2.0, molecule_H2O)]],
        program="reaction",
        singlepoint_specification=sp_spec,
        optimization_specification=None,
        keywords=rxn_keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_reactions(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete


def test_reaction_full_2(fulltest_client: PortalClient):
    sp_spec = {"program": "psi4", "driver": "energy", "method": "b3lyp", "basis": "def2-tzvp", "keywords": {}}

    opt_spec = {
        "program": "geometric",
        "keywords": {},
        "qc_specification": {"program": "psi4", "method": "pbe0", "basis": "sto-3g", "keywords": {}},
    }

    rxn_keywords = {}

    meta, ids = fulltest_client.add_reactions(
        stoichiometries=[[(-2.0, molecule_H2), (-1.0, molecule_O2), (2.0, molecule_H2O)]],
        program="reaction",
        singlepoint_specification=sp_spec,
        optimization_specification=opt_spec,
        keywords=rxn_keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_reactions(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete


def test_reaction_full_3(fulltest_client: PortalClient):
    opt_spec = {
        "program": "geometric",
        "keywords": {},
        "qc_specification": {
            "program": "psi4",
            "driver": "energy",
            "method": "pbe0",
            "basis": "def2-tzvp",
            "keywords": {},
        },
    }

    rxn_keywords = {}

    meta, ids = fulltest_client.add_reactions(
        stoichiometries=[[(-2.0, molecule_H2), (-1.0, molecule_O2), (2.0, molecule_H2O)]],
        program="reaction",
        singlepoint_specification=None,
        optimization_specification=opt_spec,
        keywords=rxn_keywords,
    )

    for i in range(360):
        time.sleep(1)
        rec = fulltest_client.get_reactions(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
