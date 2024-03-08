from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.torsiondrive import TorsiondriveDataset
from qcportal.record_models import PriorityEnum
from .testing_helpers import load_test_data

if TYPE_CHECKING:
    from qcportal import PortalClient


@pytest.fixture(scope="function")
def torsiondrive_ds(submitter_client: PortalClient):
    ds = submitter_client.add_dataset(
        "torsiondrive",
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
    assert ds.owner_user == ds._client.username
    assert ds.owner_group == "group1"

    yield ds


@pytest.mark.parametrize("find_existing", [True, False])
def test_torsiondrive_dataset_client_submit(torsiondrive_ds: TorsiondriveDataset, find_existing: bool):
    input_spec_1, molecule_1, _ = load_test_data("td_H2O2_mopac_pm6")

    torsiondrive_ds.add_entry(name="test_molecule", initial_molecules=molecule_1)
    torsiondrive_ds.add_specification("test_spec", input_spec_1, "test_specification")

    torsiondrive_ds.submit()
    assert torsiondrive_ds.status()["test_spec"]["waiting"] == 1

    torsiondrive_ds.submit()
    assert torsiondrive_ds.status()["test_spec"]["waiting"] == 1

    # Should only be one record
    record_id = 0
    for e, s, r in torsiondrive_ds.iterate_records():
        assert r.owner_user == torsiondrive_ds.owner_user
        assert r.owner_group == torsiondrive_ds.owner_group
        record_id = r.id

    # delete & re-add entry, then resubmit
    torsiondrive_ds.delete_entries(["test_molecule"])
    assert torsiondrive_ds.status() == {}

    # record still on the server?
    r = torsiondrive_ds._client.get_records(record_id)
    assert r.owner_user == torsiondrive_ds.owner_user

    # now resubmit
    torsiondrive_ds.add_entry(name="test_molecule", initial_molecules=molecule_1)
    torsiondrive_ds.submit(find_existing=find_existing)
    assert torsiondrive_ds.status()["test_spec"]["waiting"] == 1

    for e, s, r in torsiondrive_ds.iterate_records():
        assert r.owner_user == torsiondrive_ds.owner_user
        assert r.owner_group == torsiondrive_ds.owner_group

        if find_existing:
            assert r.id == record_id
        else:
            assert r.id != record_id
