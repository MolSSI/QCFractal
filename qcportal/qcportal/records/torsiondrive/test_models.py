from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.records.torsiondrive.testing_helpers import run_test_data, load_test_data
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


all_includes = ["initial_molecules", "optimizations"]


@pytest.mark.parametrize("includes", [None, all_includes])
def test_torsiondriverecord_model(
    storage_socket: SQLAlchemySocket,
    snowflake_client: PortalClient,
    activated_manager_name: ManagerName,
    includes: Optional[List[str]],
):

    input_spec, molecules, results = load_test_data("td_H2O2_psi4_b3lyp")

    rec_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_psi4_b3lyp")
    record = snowflake_client.get_torsiondrives(rec_id, include=includes)

    if includes is not None:
        record.client = None
        assert record.offline

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "torsiondrive"
    assert record.specification == input_spec

    assert len(molecules) == len(record.initial_molecules)
    assert molecules[0] == record.initial_molecules[0]

    # get all optimizations first
    opts_1 = record.optimizations
    min_opts_1 = record.minimum_optimizations
    assert sum(len(o) for o in opts_1.values()) == len(results)
    assert set(opts_1.keys()) == set(min_opts_1.keys())

    # Get minimum opts first
    record = snowflake_client.get_torsiondrives(rec_id, include=includes)

    min_opts_2 = record.minimum_optimizations
    opts_2 = record.optimizations
    assert sum(len(o) for o in opts_2.values()) == len(results)
    assert set(opts_2.keys()) == set(min_opts_2.keys())

    # Did we get the same minimum optimizations?
    assert {k: v.id for k, v in min_opts_1.items()} == {k: v.id for k, v in min_opts_2.items()}
