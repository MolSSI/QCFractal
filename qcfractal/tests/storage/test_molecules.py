"""
Tests the molecule subsocket
"""

import pytest
import qcfractal.interface as ptl


def to_molecule(mol_dict):
    mol_dict = mol_dict.copy()
    mol_dict.pop("molecular_formula", None)
    mol_dict.pop("molecule_hash", None)
    return ptl.models.Molecule(**mol_dict)


# TODO - needs lots more test cases with lots of different fields filled out


def test_molecules_basic(storage_socket):

    water = ptl.data.get_molecule("water_dimer_minima.psimol")

    # Add once
    meta, ids = storage_socket.molecule.add([water])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0
    assert meta.inserted_idx == [0]

    # Now get the molecule by id
    mols = storage_socket.molecule.get(ids)
    mols = [to_molecule(x) for x in mols]
    assert len(mols) == 1
    assert mols[0].get_hash() == water.get_hash()
    assert water.compare(mols[0])

    # Is a valid molecule object dictionary
    assert mols[0].validated

    # Try to add again
    meta, ids2 = storage_socket.molecule.add([water])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]

    # Delete the molecule
    meta = storage_socket.molecule.delete(ids)
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.deleted_idx == [0]

    # Make sure it is gone
    # This should return a list containing only None
    mols = storage_socket.molecule.get(ids, missing_ok=True)
    assert mols == [None]


def test_molecules_add_with_id(storage_socket):
    # Adding with an id already set is ok - the returned id may not
    # be the same

    bad_id = 99998888
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water = water.copy(update={"id": bad_id})

    meta, ids = storage_socket.molecule.add([water])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 0
    assert meta.inserted_idx == [0]
    assert ids[0] != water.id

    # Getting by the old id shouldn't work
    with pytest.raises(RuntimeError, match=r"Could not find all requested molecule records"):
        storage_socket.molecule.get([bad_id], missing_ok=False)

    # Getting by the new id shouldn't work
    mols = storage_socket.molecule.get(ids, missing_ok=False)
    assert mols[0]["id"] == ids[0]
    assert to_molecule(mols[0]).compare(water)


def test_molecules_add_duplicates_1(storage_socket):
    # Tests various ways of adding duplicate molecules
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")

    meta, ids = storage_socket.molecule.add([water, hooh, water, hooh, hooh])
    assert meta.success
    assert meta.n_inserted == 2
    assert meta.n_existing == 3
    assert meta.inserted_idx == [0, 1]
    assert meta.existing_idx == [2, 3, 4]
    assert ids[0] == ids[2]
    assert ids[1] == ids[3]
    assert ids[1] == ids[4]

    # Test in a different order
    meta, ids = storage_socket.molecule.add([hooh, hooh, water, water, hooh])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 5
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0, 1, 2, 3, 4]
    assert ids[0] == ids[1]
    assert ids[0] == ids[4]
    assert ids[2] == ids[3]


def test_molecules_get_duplicates(storage_socket):
    # Tests various ways of getting duplicate molecules
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")

    meta, ids = storage_socket.molecule.add([water, hooh])
    assert meta.success
    assert meta.n_inserted == 2

    id1, id2 = ids
    mols = storage_socket.molecule.get([id1, id2, id1, id2, id1])
    mols = [to_molecule(x) for x in mols]
    assert len(mols) == 5
    assert mols[0].compare(mols[2])
    assert mols[0].compare(mols[4])
    assert mols[1].compare(mols[3])
    assert mols[0].id == mols[2].id
    assert mols[0].id == mols[4].id
    assert mols[1].id == mols[3].id

    # Try getting in a different order
    mols = storage_socket.molecule.get([id2, id1, id1, id1, id2])
    mols = [to_molecule(x) for x in mols]
    assert len(mols) == 5
    assert mols[0].compare(mols[4])
    assert mols[1].compare(mols[2])
    assert mols[1].compare(mols[3])
    assert mols[0].id == mols[4].id
    assert mols[1].id == mols[2].id
    assert mols[1].id == mols[3].id


def test_molecules_delete_nonexist(storage_socket):

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    meta, ids = storage_socket.molecule.add([water])
    assert meta.success

    meta = storage_socket.molecule.delete([456, ids[0], ids[0], 123, 789])
    assert meta.success
    assert meta.n_deleted == 1
    assert meta.n_missing == 4
    assert meta.missing_idx == [0, 2, 3, 4]
    assert meta.deleted_idx == [1]


def test_molecules_get_nonexist(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")

    meta, ids = storage_socket.molecule.add([water, hooh])
    assert meta.success

    id1, id2 = ids
    meta = storage_socket.molecule.delete([id1])
    assert meta.success

    # We now have one molecule in the database and one that has been deleted
    # Try to get both with missing_ok = True. This should have None in the returned list
    mols = storage_socket.molecule.get([id1, id2, id1, id2], missing_ok=True)
    assert len(mols) == 4
    assert mols[0] is None
    assert mols[2] is None
    assert hooh.compare(to_molecule(mols[1]))
    assert hooh.compare(to_molecule(mols[3]))

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(RuntimeError, match=r"Could not find all requested molecule records"):
        storage_socket.molecule.get([id1, id2, id1, id2], missing_ok=False)


def test_molecules_add_mixed_1(storage_socket):
    # Tests a simple add_mixed
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")
    ne4 = ptl.data.get_molecule("neon_tetramer.psimol")

    meta, ids = storage_socket.molecule.add_mixed([hooh, water, hooh, water, water, ne4])
    assert meta.success
    assert meta.n_inserted == 3
    assert meta.n_existing == 3
    assert ids[0] == ids[2]
    assert ids[1] == ids[3]
    assert ids[1] == ids[4]
    assert meta.inserted_idx == [0, 1, 5]
    assert meta.existing_idx == [2, 3, 4]

    meta, ids = storage_socket.molecule.add_mixed([ids[0], ids[0], ids[1], ids[1], ids[0]])
    assert meta.success
    assert meta.n_inserted == 0
    assert meta.n_existing == 5
    assert ids[0] == ids[1]
    assert ids[2] == ids[3]
    assert ids[0] == ids[4]


def test_molecules_add_mixed_2(storage_socket):
    # Tests a simple add_mixed
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")
    ne4 = ptl.data.get_molecule("neon_tetramer.psimol")

    meta, ids = storage_socket.molecule.add([water])
    assert meta.success

    meta, new_ids = storage_socket.molecule.add_mixed([hooh, ids[0]])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1
    assert ids[0] == new_ids[1]

    # Try with duplicates, including a duplicate specified by molecule and by id
    meta, new_ids_2 = storage_socket.molecule.add_mixed([ne4, hooh, ids[0], new_ids[0], water, ids[0]])
    assert meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 5
    assert meta.inserted_idx == [0]
    assert new_ids_2[1] == new_ids[0]
    assert new_ids_2[2] == ids[0]
    assert new_ids_2[3] == new_ids[0]
    assert new_ids_2[4] == ids[0]
    assert new_ids_2[5] == ids[0]

    mols = storage_socket.molecule.get([ids[0], new_ids[0], new_ids[1]], missing_ok=False)
    mols = [to_molecule(x) for x in mols]
    assert mols[0].compare(water)
    assert mols[1].compare(hooh)
    assert mols[2].compare(water)


def test_molecules_add_mixed_bad_1(storage_socket):
    # Tests add_mixed but giving it a nonexistant id
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")

    meta, ids = storage_socket.molecule.add([water])
    assert meta.success

    meta, new_ids = storage_socket.molecule.add_mixed([hooh, 12345, 67890, water])
    assert not meta.success
    assert meta.n_inserted == 1
    assert meta.n_existing == 1
    assert len(meta.errors) == 2
    assert ids[0] == new_ids[3]
    assert meta.error_idx == [1, 2]
    assert "MoleculeORM object with id=12345 was not found" in meta.errors[0][1]
    assert "MoleculeORM object with id=67890 was not found" in meta.errors[1][1]


def test_molecules_query(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")

    added_mols = [water, hooh]
    added_mols = sorted(added_mols, key=lambda x: x.get_hash())

    meta, ids = storage_socket.molecule.add(added_mols)
    assert meta.success
    assert meta.inserted_idx == [0, 1]

    #################################################
    # Note that queries may not be returned in order
    #################################################

    # Query by id
    meta, mols = storage_socket.molecule.query(id=ids)
    mols = [to_molecule(x) for x in mols]
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert meta.n_returned == 2
    assert mols[0].compare(added_mols[0])
    assert mols[1].compare(added_mols[1])

    # Query by hash
    meta, mols = storage_socket.molecule.query(molecule_hash=[water.get_hash(), hooh.get_hash()])
    mols = [to_molecule(x) for x in mols]
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert meta.n_returned == 2
    assert meta.success
    assert meta.n_returned == 2
    assert mols[0].compare(added_mols[0])
    assert mols[1].compare(added_mols[1])

    # Query by formula
    meta, mols = storage_socket.molecule.query(molecular_formula=["H4O2", "H2O2"])
    mols = [to_molecule(x) for x in mols]
    mols = sorted(mols, key=lambda x: x.get_hash())
    assert meta.success
    assert meta.n_returned == 2
    assert mols[0].compare(added_mols[0])
    assert mols[1].compare(added_mols[1])

    # Queries should be intersections
    meta, mols = storage_socket.molecule.query(molecular_formula=["H4O2", "H2O2"], molecule_hash=[water.get_hash()])
    mols = [to_molecule(x) for x in mols]
    assert meta.success
    assert meta.n_returned == 1
    assert mols[0].compare(water)


def test_molecules_query_limit(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    hooh = ptl.data.get_molecule("hooh.json")

    added_mols = [water, hooh]

    meta, ids = storage_socket.molecule.add(added_mols)
    assert meta.success

    # Query by hash
    all_found = []

    meta, mols = storage_socket.molecule.query(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1)
    assert meta.success
    assert meta.n_returned == 1
    assert len(mols) == 1
    all_found.extend(mols)

    meta, mols = storage_socket.molecule.query(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1, skip=1)
    assert meta.success
    assert meta.n_returned == 1
    assert len(mols) == 1
    all_found.extend(mols)

    # Asking for more molecules than there are
    meta, mols = storage_socket.molecule.query(molecule_hash=[water.get_hash(), hooh.get_hash()], limit=1, skip=2)
    assert meta.success
    assert meta.n_returned == 0
    assert len(mols) == 0
