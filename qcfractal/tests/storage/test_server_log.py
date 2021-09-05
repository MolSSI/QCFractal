"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

import pytest
from datetime import datetime

from .test_procedure import load_procedure_data


@pytest.fixture(scope="function")
def storage_results(storage_socket):

    proc_inputs = [
        "psi4_benzene_energy_1",
        "psi4_fluoroethane_wfn",
        "psi4_peroxide_energy_fail_kw",
        "psi4_water_gradient",
        "rdkit_water_gradient",
        "psi4_benzene_energy_2",
        "psi4_methane_gradient_fail_iter",
        "psi4_peroxide_energy_fail_method",
        "psi4_water_hessian",
        "psi4_benzene_energy_3",
        "psi4_methane_opt_fail_qcmethod",
        "psi4_peroxide_energy_wfn",
        "psi4_water_opt_fail_scfiter",
        "psi4_benzene_opt",
        "psi4_methane_opt_sometraj",
        "psi4_peroxide_opt_fail_optiter",
        "rdkit_benzene_energy",
        "psi4_fluoroethane_opt_notraj",
        "psi4_peroxide_energy_fail_basis",
        "psi4_water_energy",
        "rdkit_water_energy",
    ]

    for proc in proc_inputs:
        inp, mol, _ = load_procedure_data(proc)
        storage_socket.task.create([mol], inp)

    yield storage_socket


def test_server_log(storage_results):

    # Add something to double check the test
    mol_names = ["water_dimer_minima.psimol", "water_dimer_stretch.psimol", "water_dimer_stretch2.psimol"]

    storage_results.serverinfo.update_stats()
    _, ret = storage_results.serverinfo.query_stats(limit=1)
    assert ret[0]["db_table_size"] > 0
    assert ret[0]["db_total_size"] > 0

    for row in ret[0]["db_table_information"]["rows"]:
        if row[0] == "molecule":
            assert row[2] >= 1000

    # Check queries
    now = datetime.utcnow()
    meta, ret = storage_results.serverinfo.query_stats(after=now)
    assert meta.success
    assert meta.n_returned == 0
    assert meta.n_found == 0
    assert len(ret) == 0

    meta, ret = storage_results.serverinfo.query_stats(before=now)
    assert meta.success
    assert meta.n_returned > 0
    assert meta.n_found > 0
    assert len(ret) > 0

    # Make sure we are sorting correctly
    storage_results.serverinfo.update_stats()
    meta, ret = storage_results.serverinfo.query_stats(limit=1)
    assert meta.success
    assert meta.n_found > 1
    assert meta.n_returned == 1
    assert ret[0]["timestamp"] > now

    # Test get last stats
    ret2 = storage_results.serverinfo.get_latest_stats()
    assert ret[0] == ret2
