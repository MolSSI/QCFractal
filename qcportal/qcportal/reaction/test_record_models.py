from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.reaction.testing_helpers import run_test_data, load_test_data
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


all_includes = ["components"]


@pytest.mark.parametrize("includes", [None, all_includes])
def test_reactionrecord_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, stoichiometry, results = load_test_data("rxn_H2O_psi4_mp2_optsp")

    rec_id = run_test_data(storage_socket, activated_manager_name, "rxn_H2O_psi4_mp2_optsp")
    record = snowflake_client.get_reactions(rec_id, include=includes)

    if includes is not None:
        record._client = None
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
