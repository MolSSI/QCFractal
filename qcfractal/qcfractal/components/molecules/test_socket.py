from __future__ import annotations

from typing import TYPE_CHECKING

from qcarchivetesting import load_molecule_data
from qcportal.molecules import Molecule, MoleculeQueryFilters

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_molecules_socket_validated_fix(storage_socket: SQLAlchemySocket):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")

    # Add once
    meta, ids = storage_socket.molecules.add([water, hooh])

    mols = storage_socket.molecules.get(ids)
    assert mols[0]["validated"] is True
    assert mols[0]["fix_com"] is True
    assert mols[0]["fix_orientation"] is True
    assert mols[1]["validated"] is True
    assert mols[1]["fix_com"] is True
    assert mols[1]["fix_orientation"] is True


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

    mols = storage_socket.molecules.get(ids, include=["symbols", "geometry", "fix_com", "fix_orientation", "validated"])
    assert len(mols) == 2
    assert set(mols[0].keys()) == {"id", "symbols", "geometry", "fix_com", "fix_orientation", "validated"}
    assert set(mols[1].keys()) == {"id", "symbols", "geometry", "fix_com", "fix_orientation", "validated"}
    assert mols[0]["validated"] is True
    assert mols[1]["validated"] is True


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
    mols = [Molecule(**x) for x in mols]
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
