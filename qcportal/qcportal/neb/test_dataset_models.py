from __future__ import annotations

from typing import TYPE_CHECKING

import qcportal.dataset_testing_helpers as ds_helpers
from qcarchivetesting import load_molecule_data
from qcportal.record_models import PriorityEnum
from qcportal.singlepoint.record_models import QCSpecification
from qcportal.neb import NEBDatasetNewEntry, NEBKeywords, NEBSpecification

if TYPE_CHECKING:
    from qcportal import PortalClient

test_entries = [
    NEBDatasetNewEntry(
        name="HCN",
        initial_chain=[load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)],
        #additional_keywords={'kw1': 123},
        ),
    NEBDatasetNewEntry(
        name="C3H2N",
        initial_chain=[load_molecule_data("neb/neb_C3H2N_%i" % i) for i in range(21)],
        #additional_keywords={'kw2': 456},
        ),
    NEBDatasetNewEntry(
        name="C4H3N2",
        initial_chain=[load_molecule_data("neb/neb_C4H3N2_%i" % i) for i in range(21)],
        #additional_keywords={'kw3': 789},
    ),
]

test_specs = [
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=1,
            coordinate_system='tric',
            energy_weighted=None,
            optimize_endpoints=True,
            maximum_force = 0.05,
            average_force = 0.025,
            optimize_ts = True,
            align_chain = False,
            epsilon = 1e-5,
            hessian_reset = True,
            spring_type = 0,
        ),
        singlepoint_specification=QCSpecification(
                program="psi4",
                driver="gradient",
                method="hf",
                basis="6-31g",
            keywords={"qc_kw_1": 123, "qc_kw_2": "a string"},
        ),
    ),
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=5,
            coordinate_system='tric',
            energy_weighted=None,
            optimize_endpoints=True,
            maximum_force=0.05,
            average_force=0.025,
            optimize_ts=True,
            align_chain=False,
            epsilon=1e-5,
            hessian_reset=True,
            spring_type=1,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            driver="gradient",
            method="b3lyp",
            basis="6-31g",
            keywords={"qc_kw_1": 456, "qc_kw_2": "a string"},
        ),
    ),
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=7,
            spring_constant=10,
            coordinate_system='tric',
            energy_weighted=None,
            optimize_endpoints=True,
            maximum_force=0.05,
            average_force=0.025,
            optimize_ts=True,
            align_chain=False,
            epsilon=1e-5,
            hessian_reset=True,
            spring_type=0,
        ),
        singlepoint_specification=QCSpecification(
            program="psi4",
            driver="gradient",
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
    assert sorted(rec.initial_chain, key=lambda x: x.get_hash()) == sorted(
        ent.initial_chain, key=lambda x: x.get_hash()
    )

    # Merge optimization keywords
    merged_spec = spec.dict()
    merged_spec["singlepoint_specification"]["keywords"].update(ent.additional_singlepoint_keywords)
    merged_spec["keywords"].update(ent.additional_keywords)

    assert rec.specification == NEBSpecification(**merged_spec)


def test_neb_dataset_model_add_get_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_get_entry(snowflake_client, ds, test_entries, entry_extra_compare)


def test_neb_dataset_model_add_entry_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_entry_duplicate(snowflake_client, ds, test_entries, entry_extra_compare)


def test_neb_dataset_model_delete_entry(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_delete_entry(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_add_get_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_get_spec(snowflake_client, ds, test_specs)


def test_neb_dataset_model_add_spec_duplicate(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_add_spec_duplicate(snowflake_client, ds, test_specs)


def test_neb_dataset_model_delete_spec(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_delete_spec(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_remove_record(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_remove_record(snowflake_client, ds, test_entries, test_specs)


def test_neb_dataset_model_submit(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset(
        "neb", "Test dataset", default_tag="default_tag", default_priority=PriorityEnum.low
    )
    ds_helpers.run_dataset_model_submit(ds, test_entries, test_specs[0], record_compare)


def test_neb_dataset_model_submit_missing(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_submit_missing(ds)


def test_neb_dataset_model_iterate_updated(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_iterate_updated(ds, test_entries, test_specs[0])


def test_neb_dataset_model_modify_records(snowflake_client: PortalClient):
    ds = snowflake_client.add_dataset("neb", "Test dataset")
    ds_helpers.run_dataset_model_modify_records(ds, test_entries, test_specs[0])
