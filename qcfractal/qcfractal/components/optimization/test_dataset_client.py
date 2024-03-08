from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcportal.optimization import OptimizationDataset
from qcportal.record_models import PriorityEnum
from .testing_helpers import load_test_data

if TYPE_CHECKING:
    from qcportal import PortalClient


@pytest.fixture(scope="function")
def optimization_ds(submitter_client: PortalClient):
    ds = submitter_client.add_dataset(
        "optimization",
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
def test_optimization_dataset_client_submit(optimization_ds: OptimizationDataset, find_existing: bool):
    input_spec_1, molecule_1, _ = load_test_data("opt_psi4_benzene")

    optimization_ds.add_entry(name="test_molecule", initial_molecule=molecule_1)
    optimization_ds.add_specification("test_spec", input_spec_1, "test_specification")

    optimization_ds.submit()
    assert optimization_ds.status()["test_spec"]["waiting"] == 1

    optimization_ds.submit()
    assert optimization_ds.status()["test_spec"]["waiting"] == 1

    # Should only be one record
    record_id = 0
    for e, s, r in optimization_ds.iterate_records():
        assert r.owner_user == optimization_ds.owner_user
        assert r.owner_group == optimization_ds.owner_group
        record_id = r.id

    # delete & re-add entry, then resubmit
    optimization_ds.delete_entries(["test_molecule"])
    assert optimization_ds.status() == {}

    # record still on the server?
    r = optimization_ds._client.get_records(record_id)
    assert r.owner_user == optimization_ds.owner_user

    # now resubmit
    optimization_ds.add_entry(name="test_molecule", initial_molecule=molecule_1)
    optimization_ds.submit(find_existing=find_existing)
    assert optimization_ds.status()["test_spec"]["waiting"] == 1

    for e, s, r in optimization_ds.iterate_records():
        assert r.owner_user == optimization_ds.owner_user
        assert r.owner_group == optimization_ds.owner_group

        if find_existing:
            assert r.id == record_id
        else:
            assert r.id != record_id
