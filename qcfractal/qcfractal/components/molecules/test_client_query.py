from __future__ import annotations

import pytest
from qcelemental.models import Molecule

from qcportal import PortalClient


@pytest.fixture(scope="module")
def queryable_molecules_client(session_snowflake):
    client = session_snowflake.client()
    elements1 = ["h", "he", "li", "be", "b", "c", "n", "o", "f", "ne"]
    elements2 = ["na", "mg", "al", "si", "p", "s", "cl", "ar", "k", "ca"]

    all_mols = []
    for el1 in elements1:
        mols = []
        for el2 in elements2:
            for dist in range(2, 4, 1):
                m = Molecule(
                    symbols=[el1, el2],
                    geometry=[0, 0, 0, 0, 0, dist],
                    identifiers={
                        "smiles": f"madeupsmiles_{el1}_{el2}_{dist}",
                        "inchikey": f"madeupinchi_{el1}_{el2}_{dist}",
                    },
                )
                mols.append(m)

        meta, _ = client.add_molecules(mols)
        all_mols.extend(mols)
        assert meta.n_inserted == 20

    assert len(all_mols) == 200

    yield client
    session_snowflake.reset()


def test_molecules_client_query(queryable_molecules_client: PortalClient):
    def sort_molecules(m):
        return sorted(m, key=lambda x: x.get_hash())

    # Query by formula
    query_res = queryable_molecules_client.query_molecules(molecular_formula=["HNa", "CCl", "MgB"])
    mols = list(query_res)
    assert len(mols) == 6

    # Query by identifiers
    query_res = queryable_molecules_client.query_molecules(
        identifiers={"smiles": ["madeupsmiles_h_na_3", "madeupsmiles_c_s_2"]}
    )
    mols = list(query_res)
    assert len(mols) == 2

    query_res = queryable_molecules_client.query_molecules(
        identifiers={"inchikey": ["madeupinchi_c_cl_3", "madeupinchi_ne_ar_2"]}
    )
    mols = list(query_res)
    assert len(mols) == 2

    # Query by hash
    test_mols = mols[:3]
    test_hashes = [x.get_hash() for x in test_mols]
    query_res = queryable_molecules_client.query_molecules(molecule_hash=test_hashes)

    test_mols = sort_molecules(test_mols)
    res_mols = sort_molecules(query_res)
    assert test_mols == res_mols

    # Queries should be intersections
    query_res = queryable_molecules_client.query_molecules(
        molecular_formula=["HCl", "CS"], identifiers={"smiles": ["madeupsmiles_c_s_2"]}
    )
    mols = list(query_res)
    assert len(mols) == 1


def test_molecules_client_query_empty_iter(queryable_molecules_client: PortalClient):
    query_res = queryable_molecules_client.query_molecules()
    assert len(query_res._current_batch) < queryable_molecules_client.api_limits["get_molecules"]

    all_mols = list(query_res)
    assert len(all_mols) == 200


def test_molecules_client_query_limit(queryable_molecules_client: PortalClient):
    query_res = queryable_molecules_client.query_molecules(molecular_formula=["HCl", "CS"], limit=2)
    query_res_l = list(query_res)
    assert len(query_res_l) == 2

    # Limit that still requires batching
    query_res = queryable_molecules_client.query_molecules(limit=198)
    query_res_l = list(query_res)
    assert len(query_res_l) == 198
