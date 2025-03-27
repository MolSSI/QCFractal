from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import qcportal.dataset_testing_helpers as ds_helpers
from qcarchivetesting import load_molecule_data
from qcportal.dataset_testing_helpers import dataset_submit_test_client
from qcportal.neb import NEBDatasetNewEntry, NEBKeywords, NEBSpecification
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint.record_models import QCSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient

test_entries = [
    NEBDatasetNewEntry(
        name="HCN",
        initial_chain=[load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)],
    ),
    NEBDatasetNewEntry(
        name="C3H2N",
        initial_chain=[load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)],
    ),
    NEBDatasetNewEntry(
        name="C4H3N2",
        initial_chain=[load_molecule_data("neb/neb_C4H3N2_%i" % i) for i in range(21)],
        additional_keywords={"maximum_force": 1.01},
        additional_singlepoint_keywords={"maxiter": 123},
    ),
]

test_specs = [
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=7,
            spring_constant=1,
            optimize_endpoints=True,
            maximum_force=0.05,
            average_force=0.025,
            optimize_ts=True,
            epsilon=1e-5,
            spring_type=0,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            driver="deferred",
            method="hf",
            basis="6-31g",
            keywords={"qc_kw_1": 123, "qc_kw_2": "a string"},
        ),
    ),
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=7,
            spring_constant=5,
            optimize_endpoints=True,
            maximum_force=0.05,
            average_force=0.025,
            optimize_ts=True,
            epsilon=1e-5,
            spring_type=1,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            driver="deferred",
            method="b3lyp",
            basis="6-31g",
            keywords={"qc_kw_1": 456, "qc_kw_2": "a string"},
        ),
    ),
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=9,
            spring_constant=10,
            optimize_endpoints=True,
            maximum_force=0.05,
            average_force=0.025,
            optimize_ts=True,
            epsilon=1e-5,
            spring_type=0,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            driver="deferred",
            method="hf",
            basis="6-31g*",
            keywords={"qc_kw_1": 789, "qc_kw_2": "a string"},
        ),
    ),
]


def entry_extra_compare(ent1, ent2):
    assert sorted(ent1.initial_chain, key=lambda x: x.get_hash()) == sorted(
        ent2.initial_chain, key=lambda x: x.get_hash()
    )

    assert NEBKeywords(**ent1.additional_keywords) == NEBKeywords(**ent2.additional_keywords)
    assert ent1.additional_singlepoint_keywords == ent2.additional_singlepoint_keywords


def record_compare(rec, ent, spec):
    # Initial chain on the record may only be a subset
    assert set(x.get_hash() for x in rec.initial_chain) <= set(x.get_hash() for x in ent.initial_chain)

    # Merge optimization keywords
    merged_spec = spec.dict()
    merged_spec["singlepoint_specification"]["keywords"].update(ent.additional_singlepoint_keywords)
    merged_spec["keywords"].update(ent.additional_keywords)

    assert rec.specification == NEBSpecification(**merged_spec)


@pytest.mark.parametrize("background", [True, False])
def test_neb_dataset_model_add_get_entry(dataset_submit_test_client: PortalClient, background: bool):
    ds = dataset_submit_test_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(
        dataset_submit_test_client, ds, test_entries, entry_extra_compare, background
    )


def test_neb_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_neb_dataset_model_rename_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_rename_entry(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_modify_entries(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_modify_entries(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_neb_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_neb_dataset_model_rename_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_rename_spec(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_copy(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_copy(snowflake_client, "neb", test_entries, test_specs, entry_extra_compare)


def test_neb_dataset_model_copy_full(snowflake_client: PortalClient):

    ds_helpers.run_dataset_model_copy_full(snowflake_client, "neb", test_entries, test_specs, entry_extra_compare)


def test_neb_dataset_model_clone(snowflake_client: PortalClient):
    ds_helpers.run_dataset_model_clone(snowflake_client, "neb", test_entries, test_specs, entry_extra_compare)


@pytest.mark.parametrize("background", [True, False])
def test_neb_dataset_model_submit(dataset_submit_test_client: PortalClient, background):
    ds = dataset_submit_test_client.add_dataset(
        "neb", "Test dataset", default_tag="default_tag", default_priority=PriorityEnum.low, owner_group="group1"
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare, background)


def test_neb_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds, test_entries, test_specs[0])


def test_neb_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(snowflake_client, ds, test_entries, test_specs[0])


def test_neb_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
