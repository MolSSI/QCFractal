from __future__ import annotations

from typing import TYPE_CHECKING

import qcportal.datasets.testing_helpers as ds_helpers
from qcfractaltesting import load_molecule_data
from qcportal.datasets.reaction import ReactionDatasetNewEntry
from qcportal.records import PriorityEnum
from qcportal.records.optimization.models import OptimizationSpecification, OptimizationProtocols
from qcportal.records.reaction import ReactionSpecification
from qcportal.records.singlepoint.models import QCSpecification

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
    stoich_tmp = [(x.coefficient, x.molecule) for x in ent1.stoichiometries]
    assert sorted(stoich_tmp) == sorted(ent2.stoichiometries)


def record_compare(rec, ent, spec):
    stoich_1 = set((x.coefficient, x.molecule.get_hash()) for x in rec.components)
    stoich_2 = set((x[0], x[1].get_hash()) for x in ent.stoichiometries)
    assert stoich_1 == stoich_2

    # TODO - ignores keywords (because there aren't any!)
    assert rec.specification.dict(exclude={"keywords"}) == spec.dict(exclude={"keywords"})


def test_reaction_dataset_model_add_get_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(snowflake_client, ds, test_entries, entry_extra_compare)


def test_reaction_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_reaction_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_reaction_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_reaction_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_reaction_dataset_model_submit(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset(
        "reaction", "Test dataset", default_tag="default_tag", default_priority=PriorityEnum.low
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare)


def test_reaction_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds)


def test_reaction_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(ds, test_entries, test_specs[0])


def test_reaction_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("reaction", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
