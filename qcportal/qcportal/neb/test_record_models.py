from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.neb.testing_helpers import run_test_data, load_test_data
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake

all_includes = ["initial_chain", "singlepoints", "optimizations", "molecule", "comments", "initial_molecule"]


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_neb_record_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, molecules, results = load_test_data("neb_HCN_psi4_pbe_opt2")

    rec_id = run_test_data(storage_socket, activated_manager_name, "neb_HCN_psi4_pbe_opt2")
    record = snowflake_client.get_nebs(rec_id, include=includes)

    if includes is not None:
        assert record.initial_chain_ is not None
        assert record.initial_chain_molecule_ids_ is not None
        assert record.singlepoints_ is not None
        assert record.optimizations_ is not None
        record.propagate_client(None)
        assert record.offline

        # children have all data fetched
        assert all(x.initial_molecule_ is not None for x in record.optimizations.values())
        assert all(x.comments_ is not None for x in record.optimizations.values())

        for sp in record.singlepoints.values():
            assert all(x.molecule_ is not None for x in sp)
            assert all(x.comments_ is not None for x in sp)
    else:
        assert record.initial_chain_ is None
        assert record.initial_chain_molecule_ids_ is None
        assert record.singlepoints_ is None
        assert record.optimizations_ is None

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "neb"
    assert record.specification == input_spec

    assert len(molecules) == len(record.initial_chain)
    for x, y in zip(molecules, record.initial_chain):
        assert x == y

    assert len(record.singlepoints) > 0

    assert all(len(sp) == len(molecules) for sp in record.singlepoints.values())

    ts_hessian = record.ts_hessian
    if ts_hessian is not None:
        assert ts_hessian.specification.driver == "hessian"

    assert "initial" in record.optimizations
    assert "final" in record.optimizations
    assert "transition" in record.optimizations
