from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.records.reaction.testing_helpers import run_test_data, load_test_data
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


all_includes = ["components"]


@pytest.mark.parametrize("includes", [None, all_includes])
def test_reactionrecord_model(
    storage_socket: SQLAlchemySocket,
    snowflake_client: PortalClient,
    activated_manager_name: ManagerName,
    includes: Optional[List[str]],
):

    input_spec, molecule, results = load_test_data("rxn_H2O_psi4_mp2_optsp")

    rec_id = run_test_data(storage_socket, activated_manager_name, "rxn_H2O_psi4_mp2_optsp")
    record = snowflake_client.get_reactions(rec_id, include=includes)

    if includes is not None:
        record.client = None
        assert record.offline

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "reaction"
    assert record.specification == input_spec

    assert record.total_energy < 0.0

    com = record.components
    assert len(com) > 2

    for c in com:
        if c.singlepoint_id is not None:
            assert c.singlepoint_record.id == c.singlepoint_id
        if c.optimization_id is not None:
            assert c.optimization_record.id == c.optimization_id
