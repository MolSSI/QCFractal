from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.manybody.testing_helpers import run_test_data, load_test_data
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


all_includes = ["initial_molecule", "clusters", "molecule", "comments"]


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_manybody_record_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, molecule, results = load_test_data("mb_cp_he4_psi4_mp2")

    rec_id = run_test_data(storage_socket, activated_manager_name, "mb_cp_he4_psi4_mp2")
    record = snowflake_client.get_manybodys(rec_id, include=includes)

    if includes is not None:
        assert record.initial_molecule_ is not None
        assert record.clusters_meta_ is not None
        assert record._clusters is not None
        record.propagate_client(None)
        assert record.offline

        # children have all data fetched
        for cl in record.clusters:
            assert cl.singlepoint_id is not None
            assert cl.singlepoint_record is not None
            assert cl.singlepoint_record.molecule_ is not None
            assert cl.singlepoint_record.comments_ is not None
    else:
        assert record.initial_molecule_ is None
        assert record.clusters_meta_ is None
        assert record._clusters is None

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "manybody"
    assert record.specification == input_spec

    assert molecule == record.initial_molecule

    assert isinstance(record.properties, dict)
    assert len(record.properties) > 0

    cl = record.clusters
    assert isinstance(cl, list)
    assert len(cl) > 1
    assert all(x.singlepoint_id == x.singlepoint_record.id for x in cl)
