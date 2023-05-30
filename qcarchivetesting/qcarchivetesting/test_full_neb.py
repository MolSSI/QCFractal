from __future__ import annotations

import time
from typing import TYPE_CHECKING

from qcportal.neb import NEBKeywords
from qcportal.optimization import OptimizationSpecification
from qcportal.record_models import RecordStatusEnum
from qcarchivetesting import load_molecule_data
from qcportal.singlepoint import QCSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_neb_full_1(fulltest_client: PortalClient):
    chain = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    neb_keywords = NEBKeywords(
        images=11,
        spring_constant=1,
        optimize_endpoints=True,
        maximum_force=0.02,
        average_force=0.02,
        optimize_ts=True,
        epsilon=1e-6,
        hessian_reset=True,
        spring_type=0,
    )

    sp_spec = QCSpecification(
        program="psi4",
        driver="gradient",
        method="hf",
        basis="6-31g",
        keywords={},
    )

    opt_spec = OptimizationSpecification(
        program="geometric",
        qc_specification=sp_spec,
    )

    meta, ids = fulltest_client.add_nebs(
        initial_chains=[chain],
        program="geometric",
        singlepoint_specification=sp_spec,
        optimization_specification=opt_spec,
        keywords=neb_keywords,
    )

    for i in range(600):
        time.sleep(15)
        rec = fulltest_client.get_nebs(ids[0])
        if rec.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            break
    else:
        raise RuntimeError("Did not finish calculation in time")

    assert rec.status == RecordStatusEnum.complete
