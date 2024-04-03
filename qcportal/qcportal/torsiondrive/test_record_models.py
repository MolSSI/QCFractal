from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.torsiondrive.testing_helpers import run_test_data, load_test_data
from qcportal.record_models import RecordStatusEnum
from qcportal.torsiondrive import deserialize_key

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


all_includes = ["initial_molecules", "optimizations", "initial_molecule", "final_molecule"]


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
def test_torsiondrive_record_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]]):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, molecules, results = load_test_data("td_H2O2_mopac_pm6")

    rec_id = run_test_data(storage_socket, activated_manager_name, "td_H2O2_mopac_pm6")
    record = snowflake_client.get_torsiondrives(rec_id, include=includes)

    if includes is not None:
        assert record.initial_molecules_ is not None
        assert record.optimizations_ is not None
        record.propagate_client(None)
        assert record.offline

        # children have all data fetched
        for opts in record._optimizations_cache.values():
            assert all(x.initial_molecule_ is not None for x in opts)
            assert all(x.final_molecule_ is not None for x in opts)
    else:
        assert record.initial_molecules_ is None
        assert record.optimizations_ is None
        assert record._optimizations_cache is None

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

    # Check that optimization keys/records match
    idx = {key: 0 for key in opts_1.keys()}  # index of the list in each key
    for optinfo in record.optimizations_:
        k = deserialize_key(optinfo.key)
        assert opts_1[k][idx[k]].id == optinfo.optimization_id
        idx[k] += 1
