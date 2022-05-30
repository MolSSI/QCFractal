from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractaltesting import load_molecule_data
from qcportal import PortalRequestError
from qcportal.molecules import Molecule, MoleculeIdentifiers

if TYPE_CHECKING:
    from qcportal import PortalClient


def test_molecules_client_basic(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")

    # Add once
    meta, ids = snowflake_client.add_molecules([water])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0
    assert meta.inserted_idx == [0]

    # Now get the molecule by id
    mols = snowflake_client.get_molecules(ids)
    assert len(mols) == 1
    assert mols[0].get_hash() == water.get_hash()
    assert water == mols[0]

    # Get as a single id
    mol = snowflake_client.get_molecules(ids[0])
    assert isinstance(mol, Molecule)
    assert water == mol

    # Is a valid molecule object dictionary
    assert mols[0].validated

    # Try to add again
    meta, ids2 = snowflake_client.add_molecules([water])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]

    # Delete the molecule
    meta = snowflake_client.delete_molecules(ids)
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.deleted_idx == [0]

    # Make sure it is gone
    # This should return a list containing only None
    mols = snowflake_client.get_molecules(ids, missing_ok=True)
    assert mols == [None]


def test_molecules_client_add_with_id(snowflake_client: PortalClient):
    # Adding with an id already set is ok - the returned id may not
    # be the same

    bad_id = 99998888
    water = load_molecule_data("water_dimer_minima")
    water = water.copy(update={"id": bad_id})

    meta, ids = snowflake_client.add_molecules([water])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0
    assert meta.inserted_idx == [0]
    assert ids[0] != water.id

    ## Getting by the old id shouldn't work
    with pytest.raises(PortalRequestError, match=r"Could not find all requested records"):
        snowflake_client.get_molecules([bad_id], missing_ok=False)

    ## Getting by the new id should work
    mols = snowflake_client.get_molecules(ids, missing_ok=False)
    assert mols[0].id == ids[0]
    assert mols[0] == water


def test_molecules_client_add_duplicates_1(snowflake_client: PortalClient):
    # Tests various ways of adding duplicate molecules
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = snowflake_client.add_molecules([water, hooh, water, hooh, hooh])
    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 3
    assert meta.inserted_idx == [0, 1]
    assert meta.existing_idx == [2, 3, 4]
    assert ids[0] == ids[2]
    assert ids[1] == ids[3]
    assert ids[1] == ids[4]

    # Test in a different order
    meta, ids = snowflake_client.add_molecules([hooh, hooh, water, water, hooh])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 5
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0, 1, 2, 3, 4]
    assert ids[0] == ids[1]
    assert ids[0] == ids[4]
    assert ids[2] == ids[3]


def test_molecules_client_get_duplicates(snowflake_client: PortalClient):
    # Tests various ways of getting duplicate molecules
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = snowflake_client.add_molecules([water, hooh])
    assert meta.success
    assert meta.n_inserted == 2

    id1, id2 = ids
    mols = snowflake_client.get_molecules([id1, id2, id1, id2, id1])
    assert len(mols) == 5
    assert mols[0] == mols[2]
    assert mols[0] == mols[4]
    assert mols[1] == mols[3]
    assert mols[0].id == mols[2].id
    assert mols[0].id == mols[4].id
    assert mols[1].id == mols[3].id

    # Try getting in a different order
    mols = snowflake_client.get_molecules([id2, id1, id1, id1, id2])
    assert len(mols) == 5
    assert mols[0] == mols[4]
    assert mols[1] == mols[2]
    assert mols[1] == mols[3]
    assert mols[0].id == mols[4].id
    assert mols[1].id == mols[2].id
    assert mols[1].id == mols[3].id


def test_molecules_client_delete_nonexist(snowflake_client: PortalClient):

    water = load_molecule_data("water_dimer_minima")
    meta, ids = snowflake_client.add_molecules([water])
    assert meta.success

    meta = snowflake_client.delete_molecules([456, ids[0], ids[0], 123, 789])
    assert meta.success is False
    assert meta.n_deleted == 1
    assert meta.n_errors == 4
    assert meta.error_idx == [0, 2, 3, 4]
    assert meta.deleted_idx == [1]


def test_molecules_client_get_nonexist(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = snowflake_client.add_molecules([water, hooh])
    assert meta.success

    id1, id2 = ids
    meta = snowflake_client.delete_molecules([id1])
    assert meta.success

    # We now have one molecule in the database and one that has been deleted
    # Try to get both with missing_ok = True. This should have None in the returned list
    mols = snowflake_client.get_molecules([id1, id2, id1, id2], missing_ok=True)
    assert len(mols) == 4
    assert mols[0] is None
    assert mols[2] is None
    assert hooh == mols[1]
    assert hooh == mols[3]

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(PortalRequestError, match=r"Could not find all requested records"):
        snowflake_client.get_molecules([id1, id2, id1, id2], missing_ok=False)


def test_molecules_client_query(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    water_dict = water.dict()
    water_dict["identifiers"] = MoleculeIdentifiers(smiles="smiles_str", inchikey="inchikey_str")
    water = Molecule(**water_dict)

    added_mols = [water, hooh]
    added_mols = sorted(added_mols, key=lambda x: x.get_hash())

    meta, ids = snowflake_client.add_molecules(added_mols)
    assert meta.success
    assert meta.inserted_idx == [0, 1]

    #################################################
    # Note that queries may not be returned in order
    #################################################

    # Query by hash
    meta, mols = snowflake_client.query_molecules(molecule_hash=[water.get_hash(), hooh.get_hash()])
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert len(mols) == 2
    assert mols[0] == added_mols[0]
    assert mols[1] == added_mols[1]

    # Query by formula
    meta, mols = snowflake_client.query_molecules(molecular_formula=["H4O2", "H2O2"])
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert len(mols) == 2
    assert mols[0] == added_mols[0]
    assert mols[1] == added_mols[1]

    # Query by identifiers
    meta, mols = snowflake_client.query_molecules(identifiers={"smiles": ["smiles_str"]})
    assert meta.success
    assert len(mols) == 1
    assert mols[0] == added_mols[0]

    # Queries should be intersections
    meta, mols = snowflake_client.query_molecules(molecular_formula=["H4O2", "H2O2"], molecule_hash=[water.get_hash()])
    assert meta.success
    assert len(mols) == 1
    assert mols[0] == water

    # Empty everything = return all
    meta, mols = snowflake_client.query_molecules()
    assert meta.n_found == 2

    # Empty lists will constrain the results to be empty
    meta, mols = snowflake_client.query_molecules(molecule_hash=[])
    assert meta.n_found == 0
    assert mols == []


def test_molecules_client_query_limit(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    added_mols = [water, hooh]

    meta, ids = snowflake_client.add_molecules(added_mols)
    assert meta.success

    meta, mols = snowflake_client.query_molecules(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1)
    assert meta.success
    assert len(mols) == 1

    meta, mols = snowflake_client.query_molecules(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1, skip=1)
    assert meta.success
    assert len(mols) == 1

    # Asking for more molecules than there are
    meta, mols = snowflake_client.query_molecules(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1, skip=2)
    assert meta.success
    assert len(mols) == 0


def test_molecules_client_get_empty(snowflake_client: PortalClient):
    assert snowflake_client.get_molecules([]) == []

    water = load_molecule_data("water_dimer_minima")
    _, ids = snowflake_client.add_molecules([water])
    assert len(ids) == 1

    assert snowflake_client.get_molecules([]) == []


def test_molecules_client_modify(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    _, ids = snowflake_client.add_molecules([water, hooh])
    assert len(ids) == 2

    meta = snowflake_client.modify_molecule(
        ids[0],
        name="water_dimer",
        comment="This is a comment",
        identifiers=MoleculeIdentifiers(
            smiles="madeupsmiles", inchi="madeupinchi", molecule_hash="notahash", molecular_formula="XXXX"
        ),
        overwrite_identifiers=False,
    )
    assert meta.success
    assert meta.updated_idx == [0]

    # Did it actually update?
    mols = snowflake_client.get_molecules(ids)
    assert mols[0].name == "water_dimer"
    assert mols[0].comment == "This is a comment"
    assert mols[0].identifiers.smiles == "madeupsmiles"
    assert mols[0].identifiers.inchi == "madeupinchi"

    # Hash & formula unchanged
    assert mols[0].identifiers.molecule_hash == water.get_hash()
    assert mols[0].identifiers.molecular_formula == water.get_molecular_formula()

    # Now try with overwrite_identifiers = True
    meta = snowflake_client.modify_molecule(
        ids[0],
        name="water_dimer 2",
        identifiers=MoleculeIdentifiers(smiles="madeupsmiles2", inchikey="madeupinchikey"),
        overwrite_identifiers=True,
    )
    assert meta.success
    assert meta.updated_idx == [0]

    # Did it actually update?
    mols = snowflake_client.get_molecules(ids)
    assert mols[0].name == "water_dimer 2"
    assert mols[0].comment == "This is a comment"
    assert mols[0].identifiers.smiles == "madeupsmiles2"
    assert "inchi" not in mols[0].identifiers
    assert mols[0].identifiers.inchikey == "madeupinchikey"

    # Hash & formula unchanged
    assert mols[0].identifiers.molecule_hash == water.get_hash()
    assert mols[0].identifiers.molecular_formula == water.get_molecular_formula()


def test_molecules_client_update_nonexist(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")

    _, ids = snowflake_client.add_molecules([water])
    assert len(ids) == 1

    with pytest.raises(PortalRequestError, match=r"Molecule with id.*not found in the database"):
        snowflake_client.modify_molecule(
            9999,
            name="water_dimer",
            comment="This is a comment",
            identifiers=MoleculeIdentifiers(smiles="madeupsmiles", inchi="madeupinchi"),
            overwrite_identifiers=False,
        )
