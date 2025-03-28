from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import qcportal.dataset_testing_helpers as ds_helpers
from qcportal.dataset_testing_helpers import dataset_submit_test_client
from qcportal.molecules import Molecule
from qcportal.optimization.record_models import OptimizationSpecification, OptimizationProtocols
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint.record_models import QCSpecification
from qcportal.torsiondrive import TorsiondriveDatasetNewEntry, TorsiondriveKeywords, TorsiondriveSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient

test_entries = [
    TorsiondriveDatasetNewEntry(
        name="hydrogen_4",
        initial_molecules=[Molecule(symbols=["h", "h", "h", "h"], geometry=[0, 0, 0, 0, 0, 2, 0, 0, 4, 0, 0, 6])],
        additional_keywords=dict(
            dihedrals=[(1, 2, 3, 4), (5, 6, 7, 8)],
            grid_spacing=[30, 60],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
    ),
    TorsiondriveDatasetNewEntry(
        name="h_4_2",
        initial_molecules=[
            Molecule(symbols=["h", "h", "h", "h"], geometry=[0, 0, 0, 0, 0, 2, 0, 0, 4, 0, 0, 6]),
            Molecule(symbols=["h", "h", "h", "h"], geometry=[0, 0, 0, 0, 0, 3, 0, 0, 6, 0, 0, 9]),
        ],
        additional_keywords=dict(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        attributes={"internal": "internal_id"},
    ),
    TorsiondriveDatasetNewEntry(
        name="ne_4",
        initial_molecules=[
            Molecule(symbols=["ne", "ne", "ne", "ne"], geometry=[0, 0, 0, 0, 0, 2, 0, 0, 4, 0, 0, 6]),
        ],
        additional_keywords=dict(
            dihedrals=[(9, 10, 11, 12)],
            grid_spacing=[5],
            dihedral_ranges=None,
            energy_decrease_thresh=0.1,
            energy_upper_limit=0.05,
        ),
        additional_optimization_keywords={"maxiter": 1234},
    ),
]

test_specs = [
    TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(),
        optimization_specification=OptimizationSpecification(
            program="opt_prog_1",
            qc_specification=QCSpecification(
                program="prog1", driver="deferred", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}
            ),
            keywords={"opt_kw_1": 123, "opt_kw_2": "a string"},
        ),
    ),
    TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(),
        optimization_specification=OptimizationSpecification(
            program="opt_prog_2",
            qc_specification=QCSpecification(
                program="prog2", driver="deferred", method="hf", basis="sto-3g", keywords={"maxiter": 40}
            ),
            keywords={"opt_kw_1": 456, "opt_kw_2": "another string"},
            protocols=OptimizationProtocols(trajectory="none"),
        ),
    ),
    TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(),
        optimization_specification=OptimizationSpecification(
            program="opt_prog_3",
            qc_specification=QCSpecification(
                program="prog3", driver="deferred", method="hf", basis="sto-3g", keywords={"maxiter": 40}
            ),
            keywords={"opt_kw_1": 789, "opt_kw_2": "another string 2"},
            protocols=OptimizationProtocols(trajectory="final"),
        ),
    ),
]


def entry_extra_compare(ent1, ent2):
    assert sorted(ent1.initial_molecules, key=lambda x: x.get_hash()) == sorted(
        ent2.initial_molecules, key=lambda x: x.get_hash()
    )

    assert TorsiondriveKeywords(**ent1.additional_keywords) == TorsiondriveKeywords(**ent2.additional_keywords)
    assert ent1.additional_optimization_keywords == ent2.additional_optimization_keywords


def record_compare(rec, ent, spec):
    assert sorted(rec.initial_molecules, key=lambda x: x.get_hash()) == sorted(
        ent.initial_molecules, key=lambda x: x.get_hash()
    )

    # Merge optimization keywords
    merged_spec = spec.dict()
    merged_spec["optimization_specification"]["keywords"].update(ent.additional_optimization_keywords)
    merged_spec["keywords"].update(ent.additional_keywords)

    assert rec.specification == TorsiondriveSpecification(**merged_spec)


@pytest.mark.parametrize("background", [True, False])
def test_torsiondrive_dataset_model_add_get_entry(dataset_submit_test_client: PortalClient, background: bool):
    ds = dataset_submit_test_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(
        dataset_submit_test_client, ds, test_entries, entry_extra_compare, background
    )


def test_torsiondrive_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_torsiondrive_dataset_model_rename_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_rename_entry(snowflake_client, ds, test_entries, test_specs)


def test_torsiondrive_dataset_model_modify_entries(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_modify_entries(snowflake_client, ds, test_entries, test_specs)


def test_torsiondrive_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_torsiondrive_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_torsiondrive_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_torsiondrive_dataset_model_rename_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_rename_spec(snowflake_client, ds, test_entries, test_specs)


def test_torsiondrive_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_torsiondrive_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_torsiondrive_dataset_model_copy(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy(snowflake_client, "torsiondrive", test_entries, test_specs, entry_extra_compare)


def test_torsiondrive_dataset_model_copy_full(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy_full(
        snowflake_client, "torsiondrive", test_entries, test_specs, entry_extra_compare
    )


def test_torsiondrive_dataset_model_clone(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_clone(snowflake_client, "torsiondrive", test_entries, test_specs, entry_extra_compare)


@pytest.mark.parametrize("background", [True, False])
def test_torsiondrive_dataset_model_submit(dataset_submit_test_client: PortalClient, background):
    ds = dataset_submit_test_client.add_dataset(
        "torsiondrive",
        "Test dataset",
        default_tag="default_tag",
        default_priority=PriorityEnum.low,
        owner_group="group1",
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare, background)


def test_torsiondrive_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds, test_entries, test_specs[0])


def test_torsiondrive_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(snowflake_client, ds, test_entries, test_specs[0])


def test_torsiondrive_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("torsiondrive", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
