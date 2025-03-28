from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import qcportal.dataset_testing_helpers as ds_helpers
from qcportal.dataset_testing_helpers import dataset_submit_test_client
from qcportal.gridoptimization import (
    GridoptimizationDatasetNewEntry,
    GridoptimizationSpecification,
    GridoptimizationKeywords,
)
from qcportal.molecules import Molecule
from qcportal.optimization.record_models import OptimizationSpecification, OptimizationProtocols
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint.record_models import QCSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient

test_entries = [
    GridoptimizationDatasetNewEntry(
        name="hydrogen_4",
        initial_molecule=Molecule(symbols=["h", "h", "h", "h"], geometry=[0, 0, 0, 0, 0, 2, 0, 0, 4, 0, 0, 6]),
        additional_keywords=dict(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
            ],
        ),
    ),
    GridoptimizationDatasetNewEntry(
        name="h_4_2",
        initial_molecule=Molecule(symbols=["h", "h", "h", "h"], geometry=[0, 0, 0, 0, 0, 3, 0, 0, 6, 0, 0, 9]),
        additional_keywords=dict(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [1, 2], "steps": [-0.2, 0.1], "step_type": "relative"},
                {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-45, 0], "step_type": "absolute"},
            ],
        ),
        attributes={"internal": "internal_id"},
    ),
    GridoptimizationDatasetNewEntry(
        name="ne_4",
        initial_molecule=Molecule(symbols=["ne", "ne", "ne", "ne"], geometry=[0, 0, 0, 0, 0, 2, 0, 0, 4, 0, 0, 6]),
        additional_keywords=dict(
            preoptimization=True,
            scans=[
                {"type": "distance", "indices": [3, 4], "steps": [-0.2, 0.1], "step_type": "relative"},
                {"type": "dihedral", "indices": [2, 3, 1, 4], "steps": [-15, 0], "step_type": "absolute"},
            ],
        ),
        additional_optimization_keywords={"maxiter": 1234},
    ),
]

test_specs = [
    GridoptimizationSpecification(
        program="gridoptimization",
        optimization_specification=OptimizationSpecification(
            program="opt_prog_1",
            qc_specification=QCSpecification(
                program="prog1", driver="deferred", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}
            ),
            keywords={"opt_kw_1": 123, "opt_kw_2": "a string"},
        ),
        keywords=GridoptimizationKeywords(),
    ),
    GridoptimizationSpecification(
        program="gridoptimization",
        optimization_specification=OptimizationSpecification(
            program="opt_prog_2",
            qc_specification=QCSpecification(
                program="prog2", driver="deferred", method="hf", basis="sto-3g", keywords={"maxiter": 40}
            ),
            keywords={"opt_kw_1": 456, "opt_kw_2": "another string"},
            protocols=OptimizationProtocols(trajectory="none"),
        ),
        keywords=GridoptimizationKeywords(),
    ),
    GridoptimizationSpecification(
        program="gridoptimization",
        optimization_specification=OptimizationSpecification(
            program="opt_prog_3",
            qc_specification=QCSpecification(
                program="prog3", driver="deferred", method="hf", basis="sto-3g", keywords={"maxiter": 40}
            ),
            keywords={"opt_kw_1": 789, "opt_kw_2": "another string 2"},
            protocols=OptimizationProtocols(trajectory="final"),
        ),
        keywords=GridoptimizationKeywords(),
    ),
]


def entry_extra_compare(ent1, ent2):
    assert ent1.initial_molecule == ent2.initial_molecule

    assert GridoptimizationKeywords(**ent1.additional_keywords) == GridoptimizationKeywords(**ent2.additional_keywords)
    assert ent1.additional_optimization_keywords == ent2.additional_optimization_keywords


def record_compare(rec, ent, spec):
    assert ent.initial_molecule == rec.initial_molecule

    # Merge optimization keywords
    merged_spec = spec.dict()
    merged_spec["optimization_specification"]["keywords"].update(ent.additional_optimization_keywords)
    merged_spec["keywords"].update(ent.additional_keywords)

    assert rec.specification == GridoptimizationSpecification(**merged_spec)


@pytest.mark.parametrize("background", [True, False])
def test_gridoptimization_dataset_model_add_get_entry(dataset_submit_test_client: PortalClient, background: bool):
    ds = dataset_submit_test_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(
        dataset_submit_test_client, ds, test_entries, entry_extra_compare, background
    )


def test_gridoptimization_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_gridoptimization_dataset_model_rename_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_rename_entry(snowflake_client, ds, test_entries, test_specs)


def test_gridoptimization_dataset_model_modify_entries(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_modify_entries(snowflake_client, ds, test_entries, test_specs)


def test_gridoptimization_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_gridoptimization_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_gridoptimization_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_gridoptimization_dataset_model_rename_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_rename_spec(snowflake_client, ds, test_entries, test_specs)


def test_gridoptimization_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_gridoptimization_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_gridoptimization_dataset_model_copy(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy(
        snowflake_client, "gridoptimization", test_entries, test_specs, entry_extra_compare
    )


def test_gridoptimization_dataset_model_copy_full(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy_full(
        snowflake_client, "gridoptimization", test_entries, test_specs, entry_extra_compare
    )


def test_gridoptimization_dataset_model_clone(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_clone(
        snowflake_client, "gridoptimization", test_entries, test_specs, entry_extra_compare
    )


@pytest.mark.parametrize("background", [True, False])
def test_gridoptimization_dataset_model_submit(dataset_submit_test_client: PortalClient, background):
    ds = dataset_submit_test_client.add_dataset(
        "gridoptimization",
        "Test dataset",
        default_tag="default_tag",
        default_priority=PriorityEnum.low,
        owner_group="group1",
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare, background)


def test_gridoptimization_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds, test_entries, test_specs[0])


def test_gridoptimization_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(snowflake_client, ds, test_entries, test_specs[0])


def test_gridoptimization_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("gridoptimization", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
