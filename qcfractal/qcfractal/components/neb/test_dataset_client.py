from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.neb.testing_helpers import test_specs
from qcportal import PortalRequestError

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_neb_dataset_client_submit_invalid_additional_keywords(snowflake_client: PortalClient):
    # A bad additional_keywords override (unknown key, forbidden by NEBKeywords' extra="forbid")
    # must be reported as a clean, user-reportable error - not an unhandled 500 - when the
    # entry is actually submitted against a specification.
    ds = snowflake_client.add_dataset("neb", "Test neb dataset")
    ds.add_specification("spec_1", test_specs[0])

    chain = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    ds.add_entry(
        name="test_entry",
        initial_chain=chain,
        additional_keywords={"this_is_not_a_real_keyword": 123},
    )

    with pytest.raises(PortalRequestError, match="Invalid additional_keywords for entry 'test_entry'"):
        ds.submit()


def test_neb_dataset_client_submit_valid_additional_keywords(snowflake_client: PortalClient):
    # Sanity check that a *valid* additional_keywords override still works fine
    ds = snowflake_client.add_dataset("neb", "Test neb dataset")
    ds.add_specification("spec_1", test_specs[0])

    chain = [load_molecule_data("neb/neb_HCN_%i" % i) for i in range(11)]
    ds.add_entry(
        name="test_entry",
        initial_chain=chain,
        additional_keywords={"maximum_cycle": 50},
    )

    ds.submit()
    assert ds.status()["spec_1"]["waiting"] == 1
