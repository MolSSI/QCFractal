from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import qcportal.dataset_testing_helpers as ds_helpers
from qcarchivetesting import load_molecule_data
from qcportal.dataset_testing_helpers import dataset_submit_test_client
from qcportal.optimization.record_models import OptimizationSpecification, OptimizationProtocols
from qcportal.reaction import ReactionDatasetNewEntry, ReactionSpecification
from qcportal.reaction.dataset_models import ReactionDatasetEntryStoichiometry
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint.record_models import QCSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient

hooh = load_molecule_data("peroxide2")
ne4 = load_molecule_data("neon_tetramer")
water = load_molecule_data("water_dimer_minima")

test_entries = [
    ReactionDatasetNewEntry(
        name="test_rxn_1",
        stoichiometries=[(2.0, water), (1.0, ne4), (-3.0, hooh)],
    ),
    ReactionDatasetNewEntry(
        name="test_rxn_2",
        stoichiometries=[(1.0, water), (-2.1, hooh)],
        comment="a comment",
        attributes={"internal": "h2"},
    ),
    ReactionDatasetNewEntry(
        name="test_rxn_3",
        stoichiometries=[(1.0, water), (-2.0, ne4)],
        additional_keywords={"abc": 1234},
    ),
]

test_specs = [
    ReactionSpecification(
        optimization_specification=OptimizationSpecification(
            program="opt_prog_1",
            qc_specification=QCSpecification(
                program="prog1", driver="deferred", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}
            ),
            keywords={"opt_kw_1": 123, "opt_kw_2": "a string"},
        ),
        keywords={},
    ),
    ReactionSpecification(
        optimization_specification=OptimizationSpecification(
            program="opt_prog_2",
            qc_specification=QCSpecification(
                program="prog2", driver="deferred", method="hf", basis="sto-3g", keywords={"maxiter": 40}
            ),
            keywords={"opt_kw_1": 456, "opt_kw_2": "another string"},
            protocols=OptimizationProtocols(trajectory="none"),
        ),
        keywords={},
    ),
    ReactionSpecification(
        optimization_specification=OptimizationSpecification(
            program="opt_prog_3",
            qc_specification=QCSpecification(
                program="prog3", driver="deferred", method="hf", basis="sto-3g", keywords={"maxiter": 40}
            ),
            keywords={"opt_kw_2": 789, "opt_kw_2": "another string 2"},
            protocols=OptimizationProtocols(trajectory="none"),
        ),
        singlepoint_specification=QCSpecification(
            program="prog3", driver="deferred", method="hf", basis="def2-tzvp", keywords={"maxiter": 40}
        ),
        keywords={},
    ),
]


def entry_extra_compare(ent1, ent2):
    if isinstance(ent1.stoichiometries[0], ReactionDatasetEntryStoichiometry):
        stoich_tmp = [(x.coefficient, x.molecule.get_hash()) for x in ent1.stoichiometries]
    else:
        stoich_tmp = [(x, y.get_hash()) for x, y in ent1.stoichiometries]

    if isinstance(ent2.stoichiometries[0], ReactionDatasetEntryStoichiometry):
        stoich_tmp_2 = [(x.coefficient, x.molecule.get_hash()) for x in ent2.stoichiometries]
    else:
        stoich_tmp_2 = [(x, y.get_hash()) for x, y in ent2.stoichiometries]

    assert sorted(stoich_tmp) == sorted(stoich_tmp_2)
    assert ent1.additional_keywords == ent2.additional_keywords


def record_compare(rec, ent, spec):
    stoich_1 = set((x.coefficient, x.molecule.get_hash()) for x in rec.components)
    stoich_2 = set((x[0], x[1].get_hash()) for x in ent.stoichiometries)
    assert stoich_1 == stoich_2

    merged_spec = spec.dict()
    merged_spec["keywords"].update(ent.additional_keywords)
    assert rec.specification == ReactionSpecification(**merged_spec)


@pytest.mark.parametrize("background", [True, False])
def test_reaction_dataset_model_add_get_entry(dataset_submit_test_client: PortalClient, background: bool):
    ds = dataset_submit_test_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(
        dataset_submit_test_client, ds, test_entries, entry_extra_compare, background
    )


def test_reaction_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_reaction_dataset_model_rename_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_rename_entry(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_modify_entries(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_modify_entries(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_reaction_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_reaction_dataset_model_rename_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_rename_spec(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_copy(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy(snowflake_client, "reaction", test_entries, test_specs, entry_extra_compare)


def test_reaction_dataset_model_copy_full(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy_full(snowflake_client, "reaction", test_entries, test_specs, entry_extra_compare)


def test_reaction_dataset_model_clone(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_clone(snowflake_client, "reaction", test_entries, test_specs, entry_extra_compare)


@pytest.mark.parametrize("background", [True, False])
def test_reaction_dataset_model_submit(dataset_submit_test_client: PortalClient, background):
    ds = dataset_submit_test_client.add_dataset(
        "reaction",
        "Test dataset",
        default_tag="default_tag",
        default_priority=PriorityEnum.low,
        owner_group="group1",
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare, background)


def test_reaction_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds, test_entries, test_specs[0])


def test_reaction_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(snowflake_client, ds, test_entries, test_specs[0])


def test_reaction_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
