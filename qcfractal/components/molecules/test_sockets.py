"""
Tests the molecule subsocket
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.exceptions import MissingDataError
from qcportal.molecules import Molecule, MoleculeIdentifiers
from qcportal.records import PriorityEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def to_molecule(mol_dict) -> Molecule:
    mol_dict = mol_dict.copy()
    mol_dict = {k: v for k, v in mol_dict.items() if v is not None}
    return Molecule(**mol_dict)


def test_molecules_socket_basic(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")

    # Add once
    meta, ids = storage_socket.molecules.add([water])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0
    assert meta.inserted_idx == [0]

    # Now get the molecule by id
    mols = storage_socket.molecules.get(ids)
    mols = [to_molecule(x) for x in mols]
    assert len(mols) == 1
    assert mols[0].get_hash() == water.get_hash()
    assert water == mols[0]

    # Is a valid molecule object dictionary
    assert mols[0].validated

    # Try to add again
    meta, ids2 = storage_socket.molecules.add([water])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]

    # Delete the molecule
    meta = storage_socket.molecules.delete(ids)
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.deleted_idx == [0]

    # Make sure it is gone
    # This should return a list containing only None
    mols = storage_socket.molecules.get(ids, missing_ok=True)
    assert mols == [None]


def test_molecules_socket_delete_inuse(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")

    input_spec, molecules, result_data = load_procedure_data("psi4_benzene_opt")

    meta, ids = storage_socket.records.optimization.add([molecules], input_spec, tag="*", priority=PriorityEnum.normal)
    assert meta.success

    # Delete the molecule
    meta = storage_socket.molecules.delete(ids)
    assert meta.success is False
    assert meta.n_deleted == 0
    assert meta.error_idx == [0]

    # Make sure it is still there
    # This should return a list containing only None
    mols = storage_socket.molecules.get(ids, missing_ok=True)
    assert mols[0] is not None


def test_molecules_socket_get_proj(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    # Add once
    meta, ids = storage_socket.molecules.add([water, hooh])

    mols = storage_socket.molecules.get(ids, include=["symbols", "geometry", "fix_com"])
    assert len(mols) == 2
    assert set(mols[0].keys()) == {"id", "symbols", "geometry", "fix_com"}
    assert set(mols[1].keys()) == {"id", "symbols", "geometry", "fix_com"}

    mols = storage_socket.molecules.get(ids, exclude=["symbols", "geometry", "fix_com"])
    assert len(mols) == 2
    assert set(mols[0].keys()).intersection({"symbols", "geometry", "fix_com"}) == set()
    assert set(mols[1].keys()).intersection({"symbols", "geometry", "fix_com"}) == set()


def test_molecules_socket_add_with_id(storage_socket: SQLAlchemySocket):
    # Adding with an id already set is ok - the returned id may not
    # be the same

    bad_id = 99998888
    water = load_molecule_data("water_dimer_minima")
    water = water.copy(update={"id": bad_id})

    meta, ids = storage_socket.molecules.add([water])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0
    assert meta.inserted_idx == [0]
    assert ids[0] != water.id

    # Getting by the old id shouldn't work
    with pytest.raises(MissingDataError, match=r"Could not find all requested records"):
        storage_socket.molecules.get([bad_id], missing_ok=False)

    # Getting by the new id shouldn't work
    mols = storage_socket.molecules.get(ids, missing_ok=False)
    assert mols[0]["id"] == ids[0]
    assert to_molecule(mols[0]) == water


def test_molecules_socket_add_duplicates_1(storage_socket: SQLAlchemySocket):
    # Tests various ways of adding duplicate molecules
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = storage_socket.molecules.add([water, hooh, water, hooh, hooh])
    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 3
    assert meta.inserted_idx == [0, 1]
    assert meta.existing_idx == [2, 3, 4]
    assert ids[0] == ids[2]
    assert ids[1] == ids[3]
    assert ids[1] == ids[4]

    # Test in a different order
    meta, ids = storage_socket.molecules.add([hooh, hooh, water, water, hooh])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 5
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0, 1, 2, 3, 4]
    assert ids[0] == ids[1]
    assert ids[0] == ids[4]
    assert ids[2] == ids[3]


def test_molecules_socket_get_duplicates(storage_socket: SQLAlchemySocket):
    # Tests various ways of getting duplicate molecules
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = storage_socket.molecules.add([water, hooh])
    assert meta.success
    assert meta.n_inserted == 2

    id1, id2 = ids
    mols = storage_socket.molecules.get([id1, id2, id1, id2, id1])
    mols = [to_molecule(x) for x in mols]
    assert len(mols) == 5
    assert mols[0] == mols[2]
    assert mols[0] == mols[4]
    assert mols[1] == mols[3]
    assert mols[0].id == mols[2].id
    assert mols[0].id == mols[4].id
    assert mols[1].id == mols[3].id

    # Try getting in a different order
    mols = storage_socket.molecules.get([id2, id1, id1, id1, id2])
    mols = [to_molecule(x) for x in mols]
    assert len(mols) == 5
    assert mols[0] == mols[4]
    assert mols[1] == mols[2]
    assert mols[1] == mols[3]
    assert mols[0].id == mols[4].id
    assert mols[1].id == mols[2].id
    assert mols[1].id == mols[3].id


def test_molecules_socket_delete_nonexist(storage_socket: SQLAlchemySocket):

    water = load_molecule_data("water_dimer_minima")
    meta, ids = storage_socket.molecules.add([water])
    assert meta.success

    meta = storage_socket.molecules.delete([456, ids[0], ids[0], 123, 789])
    assert meta.success is False
    assert meta.n_deleted == 1
    assert meta.n_errors == 4
    assert meta.error_idx == [0, 2, 3, 4]
    assert meta.deleted_idx == [1]


def test_molecules_socket_get_nonexist(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = storage_socket.molecules.add([water, hooh])
    assert meta.success

    id1, id2 = ids
    meta = storage_socket.molecules.delete([id1])
    assert meta.success

    # We now have one molecule in the database and one that has been deleted
    # Try to get both with missing_ok = True. This should have None in the returned list
    mols = storage_socket.molecules.get([id1, id2, id1, id2], missing_ok=True)
    assert len(mols) == 4
    assert mols[0] is None
    assert mols[2] is None
    assert hooh == to_molecule(mols[1])
    assert hooh == to_molecule(mols[3])

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(MissingDataError, match=r"Could not find all requested records"):
        storage_socket.molecules.get([id1, id2, id1, id2], missing_ok=False)


def test_molecules_socket_add_mixed_1(storage_socket: SQLAlchemySocket):
    # Tests a simple add_mixed
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")

    meta, ids = storage_socket.molecules.add_mixed([hooh, water, hooh, water, water, ne4])
    assert meta.success
    assert meta.n_inserted == 3
    assert meta.n_existing == 3
    assert ids[0] == ids[2]
    assert ids[1] == ids[3]
    assert ids[1] == ids[4]
    assert meta.inserted_idx == [0, 1, 5]
    assert meta.existing_idx == [2, 3, 4]

    meta, ids = storage_socket.molecules.add_mixed([ids[0], ids[0], ids[1], ids[1], ids[0]])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 5
    assert ids[0] == ids[1]
    assert ids[2] == ids[3]
    assert ids[0] == ids[4]


def test_molecules_socket_add_mixed_2(storage_socket: SQLAlchemySocket):
    # Tests a simple add_mixed
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")

    meta, ids = storage_socket.molecules.add([water])
    assert meta.success

    meta, new_ids = storage_socket.molecules.add_mixed([hooh, ids[0]])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1
    assert ids[0] == new_ids[1]

    # Try with duplicates, including a duplicate specified by molecule and by id
    meta, new_ids_2 = storage_socket.molecules.add_mixed([ne4, hooh, ids[0], new_ids[0], water, ids[0]])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 5
    assert meta.inserted_idx == [0]
    assert new_ids_2[1] == new_ids[0]
    assert new_ids_2[2] == ids[0]
    assert new_ids_2[3] == new_ids[0]
    assert new_ids_2[4] == ids[0]
    assert new_ids_2[5] == ids[0]

    mols = storage_socket.molecules.get([ids[0], new_ids[0], new_ids[1]], missing_ok=False)
    mols = [to_molecule(x) for x in mols]
    assert mols[0] == water
    assert mols[1] == hooh
    assert mols[2] == water


def test_molecules_socket_add_mixed_bad_1(storage_socket: SQLAlchemySocket):
    # Tests add_mixed but giving it a nonexistant id
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    meta, ids = storage_socket.molecules.add([water])
    assert meta.success

    meta, new_ids = storage_socket.molecules.add_mixed([hooh, 12345, 67890, water])
    assert not meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1
    assert len(meta.errors) == 2
    assert ids[0] == new_ids[3]
    assert meta.error_idx == [1, 2]
    assert "MoleculeORM object with id=12345 was not found" in meta.errors[0][1]
    assert "MoleculeORM object with id=67890 was not found" in meta.errors[1][1]


def test_molecules_socket_query(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    # TODO - could be added to the json?
    water_dict = water.dict()
    water_dict["identifiers"] = MoleculeIdentifiers(smiles="smiles_str", inchikey="inchikey_str")
    water = Molecule(**water_dict)

    added_mols = [water, hooh]
    added_mols = sorted(added_mols, key=lambda x: x.get_hash())

    meta, ids = storage_socket.molecules.add(added_mols)
    assert meta.success
    assert meta.inserted_idx == [0, 1]

    #################################################
    # Note that queries may not be returned in order
    #################################################

    # Query by id
    meta, mols = storage_socket.molecules.query(molecule_id=ids)
    mols = [to_molecule(x) for x in mols]
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert meta.n_returned == 2
    assert mols[0] == added_mols[0]
    assert mols[1] == added_mols[1]

    # Query by hash
    meta, mols = storage_socket.molecules.query(molecule_hash=[water.get_hash(), hooh.get_hash()])
    mols = [to_molecule(x) for x in mols]
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert meta.n_returned == 2
    assert mols[0] == added_mols[0]
    assert mols[1] == added_mols[1]

    # Query by formula
    meta, mols = storage_socket.molecules.query(molecular_formula=["H4O2", "H2O2"])
    mols = [to_molecule(x) for x in mols]
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert meta.n_returned == 2
    assert mols[0] == added_mols[0]
    assert mols[1] == added_mols[1]

    # Query by identifiers
    meta, mols = storage_socket.molecules.query(identifiers={"smiles": ["smiles_str"]})
    mols = [to_molecule(x) for x in mols]
    assert meta.success
    assert meta.n_returned == 1
    assert mols[0] == added_mols[0]

    # Queries should be intersections
    meta, mols = storage_socket.molecules.query(molecular_formula=["H4O2", "H2O2"], molecule_hash=[water.get_hash()])
    mols = [to_molecule(x) for x in mols]
    assert meta.success
    assert meta.n_returned == 1
    assert mols[0] == water

    # Empty everything = return all
    meta, mols = storage_socket.molecules.query()
    assert meta.n_found == 2

    # Empty lists will constrain the results to be empty
    meta, mols = storage_socket.molecules.query(molecule_id=[])
    assert meta.n_found == 0
    assert mols == []


def test_molecules_socket_query_proj(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    added_mols = [water, hooh]
    added_mols = sorted(added_mols, key=lambda x: x.get_hash())

    meta, ids = storage_socket.molecules.add(added_mols)

    #################################################
    # Note that queries may not be returned in order
    #################################################

    # Query by hash
    meta, mols = storage_socket.molecules.query(
        molecule_hash=[water.get_hash(), hooh.get_hash()], include=["geometry", "symbols"]
    )
    assert meta.success
    assert meta.n_returned == 2
    assert set(mols[0].keys()) == {"id", "symbols", "geometry"}
    assert set(mols[1].keys()) == {"id", "symbols", "geometry"}

    # Query by formula
    meta, mols = storage_socket.molecules.query(molecular_formula=["H4O2", "H2O2"], exclude=["connectivity"])
    assert meta.success
    assert meta.n_returned == 2
    assert set(mols[0].keys()).intersection({"connectivity"}) == set()
    assert set(mols[1].keys()).intersection({"connectivity"}) == set()


def test_molecules_socket_query_limit(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    added_mols = [water, hooh]

    meta, ids = storage_socket.molecules.add(added_mols)
    assert meta.success

    meta, mols = storage_socket.molecules.query(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1)
    assert meta.success
    assert meta.n_returned == 1
    assert len(mols) == 1

    meta, mols = storage_socket.molecules.query(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1, skip=1)
    assert meta.success
    assert meta.n_returned == 1
    assert len(mols) == 1

    # Asking for more molecules than there are
    meta, mols = storage_socket.molecules.query(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1, skip=2)
    assert meta.success
    assert meta.n_returned == 0
    assert len(mols) == 0


def test_molecules_socket_get_empty(storage_socket: SQLAlchemySocket):
    assert storage_socket.molecules.get([]) == []

    water = load_molecule_data("water_dimer_minima")
    _, ids = storage_socket.molecules.add([water])
    assert len(ids) == 1

    assert storage_socket.molecules.get([]) == []


def test_molecules_socket_modify(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    _, ids = storage_socket.molecules.add([water, hooh])
    assert len(ids) == 2

    meta = storage_socket.molecules.modify(
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
    mols = storage_socket.molecules.get(ids)
    assert mols[0]["name"] == "water_dimer"
    assert mols[0]["comment"] == "This is a comment"
    assert mols[0]["identifiers"]["smiles"] == "madeupsmiles"
    assert mols[0]["identifiers"]["inchi"] == "madeupinchi"

    # Hash & formula unchanged
    assert mols[0]["identifiers"]["molecule_hash"] == water.get_hash()
    assert mols[0]["identifiers"]["molecular_formula"] == water.get_molecular_formula()

    # Now try with overwrite_identifiers = True
    meta = storage_socket.molecules.modify(
        ids[0],
        name="water_dimer 2",
        identifiers=MoleculeIdentifiers(smiles="madeupsmiles2", inchikey="madeupinchikey"),
        overwrite_identifiers=True,
    )
    assert meta.success
    assert meta.updated_idx == [0]

    # Did it actually update?
    mols = storage_socket.molecules.get(ids)
    assert mols[0]["name"] == "water_dimer 2"
    assert mols[0]["comment"] == "This is a comment"
    assert mols[0]["identifiers"]["smiles"] == "madeupsmiles2"
    assert "inchi" not in mols[0]["identifiers"]
    assert mols[0]["identifiers"]["inchikey"] == "madeupinchikey"

    # Hash & formula unchanged
    assert mols[0]["identifiers"]["molecule_hash"] == water.get_hash()
    assert mols[0]["identifiers"]["molecular_formula"] == water.get_molecular_formula()


def test_molecules_socket_update_nonexist(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")

    _, ids = storage_socket.molecules.add([water])
    assert len(ids) == 1

    with pytest.raises(MissingDataError, match=r"Molecule with id.*not found in the database"):
        storage_socket.molecules.modify(
            9999,
            name="water_dimer",
            comment="This is a comment",
            identifiers=MoleculeIdentifiers(smiles="madeupsmiles", inchi="madeupinchi"),
            overwrite_identifiers=False,
        )
