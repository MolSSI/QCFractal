from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
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


def test_molecules_client_get_nonexist(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    meta, ids = snowflake_client.add_molecules([water])

    mols = snowflake_client.get_molecules(123456, missing_ok=True)
    assert mols is None

    mols = snowflake_client.get_molecules([123, 456, 789, ids[0]], missing_ok=True)

    assert len(mols) == 4
    assert mols[0] is None
    assert mols[1] is None
    assert mols[2] is None
    assert water == mols[3]

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(PortalRequestError, match=r"Could not find all requested records"):
        snowflake_client.get_molecules(123, missing_ok=False)

    with pytest.raises(PortalRequestError, match=r"Could not find all requested records"):
        snowflake_client.get_molecules([123, 456, 789, ids[0]], missing_ok=False)


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


def test_molecules_client_add_duplicates(snowflake_client: PortalClient):
    # Tests various ways of adding duplicate molecules
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    # Duplicate molecules in same add_molecules call
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
    meta, ids2 = snowflake_client.add_molecules([hooh, hooh, water, water, hooh])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 5
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0, 1, 2, 3, 4]
    assert ids2[0] == ids2[1]
    assert ids2[0] == ids2[4]
    assert ids2[2] == ids2[3]

    assert ids2[0] == ids[1]
    assert ids2[2] == ids[0]


def test_molecules_client_get_duplicates(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = snowflake_client.add_molecules([water, hooh])

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


def test_molecules_client_get_empty(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    _, ids = snowflake_client.add_molecules([water])

    assert snowflake_client.get_molecules([]) == []


def test_molecules_client_delete(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    meta, ids = snowflake_client.add_molecules([water])

    meta = snowflake_client.delete_molecules(ids)
    assert meta.success is True
    assert meta.n_deleted == 1
    assert meta.n_errors == 0
    assert meta.deleted_idx == [0]

    mol = snowflake_client.get_molecules(ids[0], missing_ok=True)
    assert mol is None


def test_molecules_client_delete_nonexist(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    meta, ids = snowflake_client.add_molecules([water])
    assert meta.success

    # Deletion succeeds if it doesn't exist
    meta = snowflake_client.delete_molecules([456, ids[0], ids[0], 123, 789])
    assert meta.success is True
    assert meta.n_deleted == 5
    assert meta.n_errors == 0
    assert meta.deleted_idx == [0, 1, 2, 3, 4]


def test_molecules_client_delete_inuse(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")

    meta, water_ids = snowflake_client.add_molecules([water])
    snowflake_client.add_singlepoints(water, "prog1", "energy", "b3lyp", "sto-3g")

    meta = snowflake_client.delete_molecules(water_ids)
    assert meta.success is False
    assert meta.n_deleted == 0
    assert meta.n_errors == 1
    assert meta.error_idx == [0]
    assert "may still be referenced" in meta.errors[0][1]


def test_molecules_client_modify(snowflake_client: PortalClient):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    _, ids = snowflake_client.add_molecules([water, hooh])
    assert len(ids) == 2

    meta = snowflake_client.modify_molecule(
        ids[0],
        name="water_dimer",
        comment="This is a comment",
        identifiers=MoleculeIdentifiers(smiles="madeupsmiles", molecule_hash="notahash", molecular_formula="XXXX"),
        overwrite_identifiers=False,
    )
    assert meta.success
    assert meta.updated_idx == [0]

    # Did it actually update?
    mols = snowflake_client.get_molecules(ids)
    assert mols[0].name == "water_dimer"
    assert mols[0].comment == "This is a comment"
    assert mols[0].identifiers.smiles == "madeupsmiles"
    assert mols[0].identifiers.inchi is None

    # Hash & formula unchanged
    assert mols[0].identifiers.molecule_hash == water.get_hash()
    assert mols[0].identifiers.molecular_formula == water.get_molecular_formula()

    # Update again
    meta = snowflake_client.modify_molecule(
        ids[0],
        name="water_dimer",
        comment="This is a comment",
        identifiers=MoleculeIdentifiers(
            inchi="madeupinchi",
        ),
        overwrite_identifiers=False,
    )
    assert meta.success
    assert meta.updated_idx == [0]

    # Updated, but did not overwrite
    mols = snowflake_client.get_molecules(ids)
    assert mols[0].name == "water_dimer"
    assert mols[0].comment == "This is a comment"
    assert mols[0].identifiers.smiles == "madeupsmiles"
    assert mols[0].identifiers.inchi == "madeupinchi"  # newly added

    # Hash & formula unchanged
    assert mols[0].identifiers.molecule_hash == water.get_hash()
    assert mols[0].identifiers.molecular_formula == water.get_molecular_formula()

    # Now try with overwrite_identifiers = True
    meta = snowflake_client.modify_molecule(
        ids[0],
        name="water_dimer 2",
        identifiers=MoleculeIdentifiers(inchikey="madeupinchikey"),
        overwrite_identifiers=True,
    )
    assert meta.success
    assert meta.updated_idx == [0]

    # Did it actually update?
    mols = snowflake_client.get_molecules(ids)
    assert mols[0].name == "water_dimer 2"
    assert mols[0].comment == "This is a comment"
    assert mols[0].identifiers.smiles is None
    assert mols[0].identifiers.inchi is None
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
