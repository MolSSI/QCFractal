from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import pytest
from qcarchivetesting import load_molecule_data

from qcportal.molecules import Molecule
from qcportal.record_models import BaseRecord

if TYPE_CHECKING:
    from qcportal import PortalClient

# The "get_*" functions take either a single id or a collection of ids, and the shape of
# what they return depends on which was given. Every collection type that make_list()
# expands must be recognized as a collection here - a set used to be treated as a single
# id, which silently returned only the first record


def _collections(ids):
    """Every kind of collection of ids the client is expected to accept"""

    return [list(ids), tuple(ids), set(ids), frozenset(ids), np.array(ids), dict.fromkeys(ids).keys()]


def test_get_molecules_id_collections(snowflake_client: PortalClient):
    mols = [load_molecule_data(n) for n in ("water_dimer_minima", "peroxide2", "neon_tetramer")]
    _, mol_ids = snowflake_client.add_molecules(mols)
    assert len(mol_ids) == 3

    for ids in _collections(mol_ids):
        fetched = snowflake_client.get_molecules(ids)
        assert isinstance(fetched, list), f"{type(ids).__name__} was treated as a single id"
        assert len(fetched) == 3
        assert {m.id for m in fetched} == set(mol_ids)

    # A single id gives back a single molecule
    single = snowflake_client.get_molecules(mol_ids[0])
    assert isinstance(single, Molecule)
    assert single.id == mol_ids[0]

    # A collection with a single id still gives back a list
    assert len(snowflake_client.get_molecules([mol_ids[0]])) == 1
    assert len(snowflake_client.get_molecules({mol_ids[0]})) == 1


def test_get_records_id_collections(snowflake_client: PortalClient):
    mols = [load_molecule_data(n) for n in ("water_dimer_minima", "peroxide2", "neon_tetramer")]
    _, rec_ids = snowflake_client.add_singlepoints(mols, "prog", "energy", "hf", "sto-3g")
    assert len(rec_ids) == 3

    for ids in _collections(rec_ids):
        fetched = snowflake_client.get_records(ids)
        assert isinstance(fetched, list), f"{type(ids).__name__} was treated as a single id"
        assert len(fetched) == 3
        assert {r.id for r in fetched} == set(rec_ids)

        # Same for the record-type-specific getters
        fetched = snowflake_client.get_singlepoints(ids)
        assert isinstance(fetched, list)
        assert {r.id for r in fetched} == set(rec_ids)

    # A single id gives back a single record
    single = snowflake_client.get_records(rec_ids[0])
    assert isinstance(single, BaseRecord)
    assert single.id == rec_ids[0]

    # A collection with a single id still gives back a list
    assert len(snowflake_client.get_records([rec_ids[0]])) == 1
    assert len(snowflake_client.get_records({rec_ids[0]})) == 1


@pytest.mark.parametrize("empty", [[], (), set(), frozenset(), np.array([], dtype=int), {}.keys()])
def test_get_records_empty_collections(snowflake_client: PortalClient, empty):
    assert snowflake_client.get_records(empty) == []
    assert snowflake_client.get_molecules(empty) == []
