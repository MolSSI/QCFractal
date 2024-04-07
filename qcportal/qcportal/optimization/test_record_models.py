from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.optimization.testing_helpers import run_test_data, load_test_data
from qcfractal.testing_helpers import compare_validate_molecule
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


all_includes = ["initial_molecule", "final_molecule", "trajectory", "molecule"]


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_optimization_record_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, molecule, result = load_test_data("opt_psi4_benzene")

    rec_id = run_test_data(storage_socket, activated_manager_name, "opt_psi4_benzene")
    record = snowflake_client.get_optimizations(rec_id, include=includes)

    if includes is not None:
        assert record.initial_molecule_ is not None
        assert record.trajectory_ids_ is not None
        record.propagate_client(None)
        assert record.offline
    else:
        assert record.initial_molecule_ is None
        assert record.trajectory_ids_ is None

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

    traj_energy = [x.properties["return_energy"] for x in traj]
    assert traj_energy == record.energies

    # Children have all their data fetched
    if includes is not None:
        assert all(x.molecule_ is not None for x in traj)
    assert all(x.molecule is not None for x in traj)
