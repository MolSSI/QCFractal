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
    """
    Full NEB test with optimizing end points and the guessed TS (result of the NEB method) optimization.
    """
    chain = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    neb_keywords = NEBKeywords(
        images=11,
        spring_constant=1,
        optimize_endpoints=True,
        maximum_force=0.05,
        average_force=0.025,
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

    ts_guess = rec.result
    initial_chain = rec.initial_chain  # List[Molecule]
    final_chain = rec.final_chain  # List[Singlepoints]
    optimizations = rec.optimizations
    singlepoints = rec.singlepoints
    ts_optimization = rec.ts_optimization
    hessian = rec.ts_hessian

    # Finding the highest energy image from the last iteration SinglepointRecords.
    last_sps = singlepoints[max(singlepoints.keys())]
    sp_energies = [(sp.properties["current energy"], sp.molecule_id) for sp in last_sps]
    energy, neb_result_id = max(sp_energies)

    # Completed?
    assert rec.status == RecordStatusEnum.complete
    # Initial chain length and number of singlepoint records from the last iteration should be 11.
    assert len(initial_chain) == 11 and len(initial_chain) == len(final_chain)
    # SinglepointRecords ids of final chain should be the same as the last iteration SinglepointRecords from rec.singlepoints.
    assert sum([1 if i.id == j.id else 0 for i, j in zip(final_chain, singlepoints[max(singlepoints.keys())])]) == 11
    # Total 3 OptimizationRecords
    assert len(optimizations) == 3
    # rec.tsoptimization should have the same id as the transition record in rec.optimizations.
    assert optimizations.get("transition").id == ts_optimization.id
    # When optimize_ts is True, SinglepointRecord containing the Hessian should exist.
    assert hessian
    # The rec.hessian should have the Hessian used for the TS optimization.
    assert hessian.properties["return_hessian"] is not None
    # And other SinglepointRecords should not have 'return_hessian'
    assert sum(1 if singlepoints[0][i].properties["return_hessian"] is None else 0 for i in range(11)) == 11
    # Result of the neb and the highest energy image of the last iteration should have the same molecule id.
    assert ts_guess.id == neb_result_id


def test_neb_full_2(fulltest_client: PortalClient):
    """
    Identical as the previous test without optimizing endpoints and transition state.
    """
    chain = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    neb_keywords = NEBKeywords(
        images=11,
        spring_constant=1,
        optimize_endpoints=False,
        maximum_force=0.05,
        average_force=0.025,
        optimize_ts=False,
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

    hessian = rec.ts_hessian  # Calling the Hessian first
    ts_guess = rec.result
    initial_chain = rec.initial_chain  # List[Molecule]
    final_chain = rec.final_chain  # List[Singlepoints]
    optimizations = rec.optimizations
    singlepoints = rec.singlepoints

    # Finding the highest energy image from the last iteration SinglepointRecords.
    last_sps = singlepoints[max(singlepoints.keys())]
    sp_energies = [(sp.properties["current energy"], sp.molecule_id) for sp in last_sps]
    energy, neb_result_id = max(sp_energies)

    # Completed?
    assert rec.status == RecordStatusEnum.complete
    # Initial chain length and number of singlepoint records from the last iteration should be 11.
    assert len(initial_chain) == 11 and len(initial_chain) == len(final_chain)
    # SinglepointRecords ids of final chain should be the same as the last iteration SinglepointRecords from rec.singlepoints.
    assert sum([1 if i.id == j.id else 0 for i, j in zip(final_chain, singlepoints[max(singlepoints.keys())])]) == 11
    # There shouldn't be any OptimizationRecords.
    assert len(optimizations) == 0
    # There should not be ts_optimization record.
    assert rec.ts_optimization is None
    # When optimize_ts is False, there should not ba a record for the Hessian.
    assert hessian is None
    # Result of the neb and the highest energy image of the last iteration should have the same molecule id.
    assert ts_guess.id == neb_result_id
