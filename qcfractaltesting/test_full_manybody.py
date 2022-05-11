"""
Full end-to-end test of singlepoint
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.records import RecordStatusEnum
from . import load_procedure_data

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_manybody_full_1(fulltest_client: PortalClient):
    input_spec, molecule, _ = load_procedure_data("mb_none_he4_psi4_mp2")

    meta, ids = fulltest_client.add_manybodys(
        initial_molecules=[molecule],
        program=input_spec.program,
        qc_specification=input_spec.qc_specification,
        keywords=input_spec.keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_manybodys(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
