from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.records.optimization.testing_helpers import run_test_data, load_test_data
from qcfractal.testing_helpers import compare_validate_molecule
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


all_includes = ["initial_molecule", "final_molecule", "trajectory"]


@pytest.mark.parametrize("includes", [None, all_includes])
def test_optimizationrecord_model(
    storage_socket: SQLAlchemySocket,
    snowflake_client: PortalClient,
    activated_manager_name: ManagerName,
    includes: Optional[List[str]],
):
    input_spec, molecule, result = load_test_data("opt_psi4_benzene")

    rec_id = run_test_data(storage_socket, activated_manager_name, "opt_psi4_benzene")
    record = snowflake_client.get_optimizations(rec_id, include=includes)

    if includes is not None:
        record.client = None
        assert record.offline

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "optimization"
    assert record.specification == input_spec

    # use compare_validate_molecule since connectivity is cleaned up for this molecule
    assert compare_validate_molecule(molecule, record.initial_molecule)
    assert compare_validate_molecule(result.final_molecule, record.final_molecule)

    assert record.energies == result.energies

    traj = record.trajectory
    assert len(traj) == len(result.trajectory)

    traj_energy = [x.properties.return_energy for x in traj]
    assert traj_energy == record.energies
