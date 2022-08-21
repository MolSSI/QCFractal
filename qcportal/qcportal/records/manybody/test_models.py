from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.records.manybody.testing_helpers import run_test_data, load_test_data
from qcportal.molecules import Molecule
from qcportal.records import RecordStatusEnum

if TYPE_CHECKING:
    from qcportal import PortalClient
    from qcfractal.db_socket import SQLAlchemySocket
    from qcportal.managers import ManagerName


all_includes = ["initial_molecule", "clusters"]


@pytest.mark.parametrize("includes", [None, all_includes])
def test_manybodyrecord_model(
    storage_socket: SQLAlchemySocket,
    snowflake_client: PortalClient,
    activated_manager_name: ManagerName,
    includes: Optional[List[str]],
):

    input_spec, molecule, results = load_test_data("mb_cp_he4_psi4_mp2")

    rec_id = run_test_data(storage_socket, activated_manager_name, "mb_cp_he4_psi4_mp2")
    record = snowflake_client.get_manybodys(rec_id, include=includes)

    if includes is not None:
        record.client = None
        assert record.offline

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "manybody"
    assert record.specification == input_spec

    assert molecule == record.initial_molecule

    assert isinstance(record.results, dict)
    assert len(record.results) > 0

    cl = record.clusters
    assert isinstance(cl, list)
    assert len(cl) > 1
    assert all(x.singlepoint_id == x.singlepoint_record.id for x in cl)
