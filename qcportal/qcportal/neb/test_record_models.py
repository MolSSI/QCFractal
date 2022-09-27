from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.neb.testing_helpers import run_test_data, load_test_data
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


all_includes = ["initial_chain", "singlepoints"]


@pytest.mark.parametrize("includes", [None, all_includes])
def test_neb_record_model(
    storage_socket: SQLAlchemySocket,
    snowflake_client: PortalClient,
    activated_manager_name: ManagerName,
    includes: Optional[List[str]],
):

    input_spec, molecules, results = load_test_data("neb_HCN_psi4_b3lyp")

    rec_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_b3lyp")
    record = snowflake_client.get_nebs(rec_id, include=includes)

    if includes is not None:
        record.client = None
        assert record.offline

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "neb"
    assert record.specification == input_spec

    assert len(molecules) == len(record.initial_chain)
    assert molecules[0] == record.initial_chain[0]

    sps_1 = record.singlepoints
    assert sum(len(o) for o in sps_1.values()) == len(results)

    # Get minimum opts first
    record = snowflake_client.get_nebs(rec_id, include=includes)

    sps_2 = record.optimizations
    assert sum(len(o) for o in sps_2.values()) == len(results)

