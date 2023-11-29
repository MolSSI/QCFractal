from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.reaction import ReactionDataset
from qcportal.record_models import PriorityEnum
from .testing_helpers import load_test_data

if TYPE_CHECKING:
    from qcportal import PortalClient


@pytest.fixture(scope="function")
def reaction_ds(submitter_client: PortalClient):
    ds = submitter_client.add_dataset(
        "reaction",
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
def test_reaction_dataset_client_submit(reaction_ds: ReactionDataset, find_existing: bool):
    input_spec_1, molecule_1, _ = load_test_data("rxn_H2O_psi4_mp2_optsp")

    reaction_ds.add_entry(name="test_molecule", stoichiometries=molecule_1)
    reaction_ds.add_specification("test_spec", input_spec_1, "test_specification")

    reaction_ds.submit()
    assert reaction_ds.status()["test_spec"]["waiting"] == 1

    reaction_ds.submit()
    assert reaction_ds.status()["test_spec"]["waiting"] == 1

    # Should only be one record
    record_id = 0
    for e, s, r in reaction_ds.iterate_records():
        assert r.owner_user == reaction_ds.owner_user
        assert r.owner_group == reaction_ds.owner_group
        record_id = r.id

    # delete & re-add entry, then resubmit
    reaction_ds.delete_entries(["test_molecule"])
    assert reaction_ds.status() == {}

    # record still on the server?
    r = reaction_ds._client.get_records(record_id)
    assert r.owner_user == reaction_ds.owner_user

    # now resubmit
    reaction_ds.add_entry(name="test_molecule", stoichiometries=molecule_1)
    reaction_ds.submit(find_existing=find_existing)
    assert reaction_ds.status()["test_spec"]["waiting"] == 1

    for e, s, r in reaction_ds.iterate_records():
        assert r.owner_user == reaction_ds.owner_user
        assert r.owner_group == reaction_ds.owner_group

        if find_existing:
            assert r.id == record_id
        else:
            assert r.id != record_id
