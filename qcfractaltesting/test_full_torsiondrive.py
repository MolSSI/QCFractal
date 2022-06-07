from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcfractal.components.records.torsiondrive.testing_helpers import load_test_data
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_torsiondrive_full_1(fulltest_client: PortalClient):
    input_data, molecules, _ = load_test_data("td_H2O2_psi4_pbe")
    meta, ids = fulltest_client.add_torsiondrives(
        initial_molecules=[molecules],
        program="torsiondrive",
        optimization_specification=input_data.optimization_specification,
        keywords=input_data.keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_torsiondrives(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
