from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import qcportal.dataset_testing_helpers as ds_helpers
from qcarchivetesting import load_molecule_data
from qcportal.dataset_testing_helpers import dataset_submit_test_client
from qcportal.manybody import ManybodyDatasetNewEntry, ManybodySpecification, BSSECorrectionEnum
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint.record_models import QCSpecification

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
        additional_singlepoint_keywords={"maxiter": 1234},
    ),
]

test_specs = [
    ManybodySpecification(
        program="qcmanybody",
        bsse_correction=[BSSECorrectionEnum.nocp, BSSECorrectionEnum.cp],
        levels={
            1: QCSpecification(
                program="prog1", driver="energy", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}
            ),
            2: QCSpecification(program="prog1", driver="energy", method="hf", basis="6-31g*", keywords={"maxiter": 20}),
        },
        keywords={"return_total_data": True},
    ),
    ManybodySpecification(
        program="qcmanybody",
        bsse_correction=[BSSECorrectionEnum.vmfc],
        levels={
            1: QCSpecification(
                program="prog2", driver="energy", method="b3lyp", basis="6-31g*", keywords={"maxiter": 20}
            ),
            2: QCSpecification(program="prog2", driver="energy", method="hf", basis="6-31g*", keywords={"maxiter": 20}),
        },
        keywords={"return_total_data": True},
    ),
    ManybodySpecification(
        program="qcmanybody",
        bsse_correction=[BSSECorrectionEnum.vmfc],
        levels={
            1: QCSpecification(
                program="prog2", driver="energy", method="b3lyp", basis="sto-3g", keywords={"maxiter": 20}
            ),
            2: QCSpecification(program="prog2", driver="energy", method="hf", basis="sto-3g", keywords={"maxiter": 20}),
        },
        keywords={"return_total_data": True},
    ),
]


def entry_extra_compare(ent1, ent2):
    assert ent1.initial_molecule == ent2.initial_molecule
    assert ent1.additional_singlepoint_keywords == ent2.additional_singlepoint_keywords


def record_compare(rec, ent, spec):
    assert rec.initial_molecule == ent.initial_molecule

    merged_spec = spec.dict()
    for v in merged_spec["levels"].values():
        v["keywords"] = v["keywords"] or {}
        v["keywords"].update(ent.additional_singlepoint_keywords)
    assert rec.specification == ManybodySpecification(**merged_spec)


@pytest.mark.parametrize("background", [True, False])
def test_manybody_dataset_model_add_get_entry(dataset_submit_test_client: PortalClient, background: bool):
    ds = dataset_submit_test_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(
        dataset_submit_test_client, ds, test_entries, entry_extra_compare, background
    )


def test_manybody_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_manybody_dataset_model_rename_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_rename_entry(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_modify_entries(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_modify_entries(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_manybody_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_manybody_dataset_model_rename_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_rename_spec(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_manybody_dataset_model_copy(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy(snowflake_client, "manybody", test_entries, test_specs, entry_extra_compare)


def test_manybody_dataset_model_copy_full(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy_full(snowflake_client, "manybody", test_entries, test_specs, entry_extra_compare)


def test_manybody_dataset_model_clone(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_clone(snowflake_client, "manybody", test_entries, test_specs, entry_extra_compare)


@pytest.mark.parametrize("background", [True, False])
def test_manybody_dataset_model_submit(dataset_submit_test_client: PortalClient, background):
    ds = dataset_submit_test_client.add_dataset(
        "manybody",
        "Test dataset",
        default_tag="default_tag",
        default_priority=PriorityEnum.low,
        owner_group="group1",
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare, background)


def test_manybody_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds, test_entries, test_specs[0])


def test_manybody_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(snowflake_client, ds, test_entries, test_specs[0])


def test_manybody_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("manybody", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
