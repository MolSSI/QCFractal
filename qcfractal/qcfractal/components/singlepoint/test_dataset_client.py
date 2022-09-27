from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.record_models import PriorityEnum
from qcportal.singlepoint import SinglepointDatasetNewEntry, SinglepointDataset
from .testing_helpers import load_test_data

if TYPE_CHECKING:
    from qcportal import PortalClient


@pytest.fixture(scope="function")
def singlepoint_ds(submitter_client: PortalClient):
    ds = submitter_client.add_dataset(
        "singlepoint",
        "Test dataset",
        "Test Description",
        "a Tagline",
        ["tag1", "tag2"],
        "new_group",
        {"prov_key_1": "prov_value_1"},
        True,
        "def_tag",
        PriorityEnum.low,
        {"meta_key_1": "meta_value_1"},
        "group1",
    )

    assert ds.owner_user is not None
    assert ds.owner_user == ds.client.username
    assert ds.owner_group == "group1"

    yield ds


def test_singlepoint_dataset_client_submit(singlepoint_ds: SinglepointDataset):

    input_spec_1, molecule_1, _ = load_test_data("sp_psi4_benzene_energy_1")

    new_ent = SinglepointDatasetNewEntry(name="test_molecule", molecule=molecule_1)
    singlepoint_ds.add_entries([new_ent])
    singlepoint_ds.add_specification("test_spec", input_spec_1, "test_specification")

    singlepoint_ds.submit()
    assert singlepoint_ds.status()["test_spec"]["waiting"] == 1

    singlepoint_ds.submit()
    assert singlepoint_ds.status()["test_spec"]["waiting"] == 1

    # Should only be one record
    record_id = 0
    for e, s, r in singlepoint_ds.iterate_records():
        assert r.owner_user == singlepoint_ds.owner_user
        assert r.owner_group == singlepoint_ds.owner_group
        record_id = r.id

    # delete & re-add entry, then resubmit
    singlepoint_ds.delete_entries(["test_molecule"])
    assert singlepoint_ds.status() == {}

    # record still on the server?
    r = singlepoint_ds.client.get_records(record_id)
    assert r.owner_user == singlepoint_ds.owner_user

    # now resubmit
    singlepoint_ds.add_entries([new_ent])
    singlepoint_ds.submit()
    assert singlepoint_ds.status()["test_spec"]["waiting"] == 1

    for e, s, r in singlepoint_ds.iterate_records():
        assert r.owner_user == singlepoint_ds.owner_user
        assert r.owner_group == singlepoint_ds.owner_group
        assert r.id == record_id
