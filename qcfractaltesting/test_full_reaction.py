from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.records import RecordStatusEnum
from . import load_record_data

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_reaction_full_1(fulltest_client: PortalClient):
    input_data, molecules, _ = load_record_data("rxn_H2O_psi4_b3lyp_sp")
    meta, ids = fulltest_client.add_reactions(
        stoichiometries=[molecules],
        program=input_data.program,
        singlepoint_specification=input_data.singlepoint_specification,
        optimization_specification=input_data.optimization_specification,
        keywords=input_data.keywords,
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
    input_data, molecules, _ = load_record_data("rxn_H2O_psi4_mp2_optsp")
    meta, ids = fulltest_client.add_reactions(
        stoichiometries=[molecules],
        program=input_data.program,
        singlepoint_specification=input_data.singlepoint_specification,
        optimization_specification=input_data.optimization_specification,
        keywords=input_data.keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_reactions(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
