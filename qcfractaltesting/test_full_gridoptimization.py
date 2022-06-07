from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcfractal.components.records.gridoptimization.testing_helpers import load_test_data
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_gridoptimization_full_1(fulltest_client: PortalClient):
    input_data, molecules, _ = load_test_data("go_H2O2_psi4_blyp")
    meta, ids = fulltest_client.add_gridoptimizations(
        initial_molecules=[molecules],
        program="gridoptimization",
        optimization_specification=input_data.optimization_specification,
        keywords=input_data.keywords,
    )

    for i in range(240):
        time.sleep(1)
        rec = fulltest_client.get_gridoptimizations(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
