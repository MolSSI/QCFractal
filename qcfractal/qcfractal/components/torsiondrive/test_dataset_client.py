from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.torsiondrive.testing_helpers import test_specs
from qcportal import PortalRequestError
from qcportal.molecules import Molecule

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_torsiondrive_dataset_client_submit_invalid_additional_keywords(snowflake_client: PortalClient):
    # A bad additional_keywords override (unknown key, forbidden by TorsiondriveKeywords'
    # extra="forbid") must be reported as a clean, user-reportable error - not an unhandled 500
    # - when the entry is actually submitted against a specification.
    ds = snowflake_client.add_dataset("torsiondrive", "Test torsiondrive dataset")
    ds.add_specification("spec_1", test_specs[0])

    mol = Molecule(symbols=["h", "h"], geometry=[0, 0, 0, 0, 0, 2])
    ds.add_entry(
        name="test_entry",
        initial_molecules=[mol],
        additional_keywords={"this_is_not_a_real_keyword": 123},
    )

    with pytest.raises(PortalRequestError, match="Invalid additional_keywords for entry 'test_entry'"):
        ds.submit()
