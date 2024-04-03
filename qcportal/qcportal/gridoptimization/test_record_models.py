from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.gridoptimization.testing_helpers import run_test_data, load_test_data
from qcportal.gridoptimization import deserialize_key
from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake

all_includes = ["initial_molecule", "starting_molecule", "optimizations", "final_molecule", "initial_molecule"]


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_gridoptimization_record_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, molecule, results = load_test_data("go_H3NS_psi4_pbe")

    rec_id = run_test_data(storage_socket, activated_manager_name, "go_H3NS_psi4_pbe")
    record = snowflake_client.get_gridoptimizations(rec_id, include=includes)

    if includes is not None:
        assert record.initial_molecule_ is not None
        assert record.optimizations_ is not None
        record.propagate_client(None)
        assert record.offline

        # children have all data fetched
        assert all(x.initial_molecule_ is not None for x in record._optimizations_cache.values())
        assert all(x.final_molecule_ is not None for x in record._optimizations_cache.values())
    else:
        assert record.initial_molecule_ is None
        assert record.optimizations_ is None
        assert record._optimizations_cache is None

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "gridoptimization"
    assert record.specification == input_spec

    assert molecule == record.initial_molecule
    assert isinstance(record.starting_molecule, Molecule)

    assert record.starting_grid == [0]

    opts = record.optimizations
    assert len(opts) == len(results)

    # ids match
    for opt_meta in record.optimizations_:
        k = deserialize_key(opt_meta.key)
        assert opts[k].id == opt_meta.optimization_id
