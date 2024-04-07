from __future__ import annotations

from typing import TYPE_CHECKING, Optional, List

import pytest

from qcfractal.components.reaction.testing_helpers import run_test_data, load_test_data
from qcportal.record_models import RecordStatusEnum

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


all_includes = ["components", "molecule", "comments", "initial_molecule", "final_molecule"]


@pytest.mark.parametrize("includes", [None, ["**"], all_includes])
@pytest.mark.parametrize("testfile", ["rxn_H2O_psi4_mp2_optsp", "rxn_H2O_psi4_mp2_opt", "rxn_H2O_psi4_b3lyp_sp"])
def test_reaction_record_model(snowflake: QCATestingSnowflake, includes: Optional[List[str]], testfile: str):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()
    activated_manager_name, _ = snowflake.activate_manager()

    input_spec, stoichiometry, results = load_test_data(testfile)

    rec_id = run_test_data(storage_socket, activated_manager_name, testfile)
    record = snowflake_client.get_reactions(rec_id, include=includes)

    if includes is not None:
        assert record.components_meta_ is not None
        assert record._components is not None
        record.propagate_client(None)
        assert record.offline

        # children have all data fetched
        for c in record.components:
            if c.singlepoint_id is not None:
                assert c.singlepoint_record is not None
                assert c.singlepoint_record.molecule_ is not None
                assert c.singlepoint_record.comments_ is not None
            if c.optimization_id is not None:
                assert c.optimization_record is not None
                assert c.optimization_record.initial_molecule_ is not None
                assert c.optimization_record.final_molecule_ is not None
                assert c.optimization_record.comments_ is not None
    else:
        assert record.components_meta_ is None
        assert record._components is None

    assert record.id == rec_id
    assert record.status == RecordStatusEnum.complete

    assert record.record_type == "reaction"
    assert record.specification == input_spec

    assert record.total_energy < 0.0

    com = record.components
    assert len(com) > 2

    for c in com:
        if c.singlepoint_id is not None:
            # Molecule id may represent the initial molecule for the optimization, not
            # necessarily the single point calculation
            if c.optimization_id is None:
                assert c.singlepoint_record.molecule.id == c.molecule_id
            else:
                assert c.singlepoint_record.molecule.id == c.optimization_record.final_molecule.id

            assert list(c.singlepoint_record.molecule.symbols) == list(c.molecule.symbols)
            assert c.singlepoint_record.id == c.singlepoint_id
        if c.optimization_id is not None:
            assert c.optimization_record.initial_molecule.id == c.molecule_id
            assert c.optimization_record.id == c.optimization_id
