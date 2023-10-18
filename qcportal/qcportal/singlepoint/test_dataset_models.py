from __future__ import annotations

from typing import TYPE_CHECKING

import qcportal.dataset_testing_helpers as ds_helpers
from qcportal.molecules import Molecule
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint import SinglepointDatasetNewEntry
from qcportal.singlepoint.record_models import QCSpecification, SinglepointProtocols

if TYPE_CHECKING:
    from qcportal import PortalClient

test_entries = [
    SinglepointDatasetNewEntry(molecule=Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, 2]), name="hydrogen_2"),
    SinglepointDatasetNewEntry(
        molecule=Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, 4]),
        name="HYDrogen_4",
        comment="a comment",
        attributes={"internal": "h2"},
        local_results={"energy": -1.0, "other_energy": -2.0},
    ),
    SinglepointDatasetNewEntry(
        molecule=Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, 6]),
        name="hydrogen_6",
        additional_keywords={"maxiter": 1000},
    ),
]

test_specs = [
    QCSpecification(program="prog1", driver="energy", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}),
    QCSpecification(
        program="prog2",
        driver="energy",
        method="hf",
        basis="sto-3g",
        keywords={"maxiter": 40},
        protocols=SinglepointProtocols(wavefunction="all"),
    ),
]


def entry_extra_compare(ent1, ent2):
    assert ent1.molecule == ent2.molecule
    assert ent1.additional_keywords == ent2.additional_keywords
    assert ent1.local_results == ent2.local_results


def record_compare(rec, ent, spec):
    assert rec.molecule == ent.molecule

    merged_spec = spec.dict()
    merged_spec["keywords"].update(ent.additional_keywords)
    assert rec.specification == QCSpecification(**merged_spec)


def test_singlepoint_dataset_model_add_get_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(snowflake_client, ds, test_entries, entry_extra_compare)


def test_singlepoint_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_singlepoint_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_singlepoint_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_singlepoint_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_singlepoint_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_singlepoint_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_singlepoint_dataset_model_submit(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset(
        "singlepoint", "Test dataset", default_tag="default_tag", default_priority=PriorityEnum.low
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare)


def test_singlepoint_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds, test_entries, test_specs[0])


def test_singlepoint_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(ds, test_entries, test_specs[0])


def test_singlepoint_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("singlepoint", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
