from __future__ import annotations

from typing import TYPE_CHECKING

import qcportal.datasets.testing_helpers as ds_helpers
from qcfractaltesting import load_molecule_data
from qcportal.datasets.manybody import ManybodyDatasetNewEntry
from qcportal.records import PriorityEnum
from qcportal.records.manybody import ManybodySpecification, ManybodyKeywords, BSSECorrectionEnum
from qcportal.records.singlepoint.models import QCSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient

water2 = load_molecule_data("water_dimer_minima")
water4 = load_molecule_data("water_stacked")

test_entries = [
    ManybodyDatasetNewEntry(
        name="test_mb_1",
        initial_molecule=water2,
    ),
    ManybodyDatasetNewEntry(
        name="test_mb_2",
        initial_molecule=water4,
        comment="a comment",
        attributes={"internal": "h2"},
    ),
    ManybodyDatasetNewEntry(
        name="test_mb_3",
        initial_molecule=water4,
        additional_keywords={"max_nbody": 1234},
    ),
]

test_specs = [
    ManybodySpecification(
        singlepoint_specification=QCSpecification(
            program="prog1", driver="energy", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}
        ),
        keywords=ManybodyKeywords(bsse_correction=BSSECorrectionEnum.none, max_nbody=4),
    ),
    ManybodySpecification(
        singlepoint_specification=QCSpecification(
            program="prog2", driver="energy", method="hf", basis="sto-3g", keywords={"maxiter": 40}
        ),
        keywords=ManybodyKeywords(bsse_correction=BSSECorrectionEnum.none),
    ),
    ManybodySpecification(
        singlepoint_specification=QCSpecification(
            program="prog3", driver="energy", method="hf", basis="sto-3g", keywords={"maxiter": 40}
        ),
        keywords=ManybodyKeywords(bsse_correction=BSSECorrectionEnum.cp),
    ),
]


def entry_extra_compare(ent1, ent2):
    assert ent1.initial_molecule == ent2.initial_molecule


def record_compare(rec, ent, spec):
    assert rec.initial_molecule == ent.initial_molecule
    assert rec.specification == spec


def test_manybody_dataset_model_add_get_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(snowflake_client, ds, test_entries, entry_extra_compare)


def test_manybody_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_manybody_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_manybody_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_manybody_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_submit(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset(
        "manybody", "Test dataset", default_tag="default_tag", default_priority=PriorityEnum.low
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare)


def test_manybody_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds)


def test_manybody_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(ds, test_entries, test_specs[0])


def test_manybody_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
