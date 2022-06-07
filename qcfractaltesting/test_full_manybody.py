from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcfractal.components.records.manybody.testing_helpers import load_test_data
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_manybody_full_1(fulltest_client: PortalClient):
    input_spec, molecule, _ = load_test_data("mb_none_he4_psi4_mp2")

    meta, ids = fulltest_client.add_manybodys(
        initial_molecules=[molecule],
        program=input_spec.program,
        singlepoint_specification=input_spec.singlepoint_specification,
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
