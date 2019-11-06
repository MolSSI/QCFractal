import time
import qcfractal
import qcelemental as qcel
import numpy as np


print("Building and clearing the database...\n")
db_name = "molecule_tests"
storage = qcfractal.storage_socket_factory(f"postgresql://localhost:5432/{db_name}")
storage._delete_DB_data(db_name)

run_tests = False
mol_trials = [1, 5, 10, 25, 50, 100, 500, 1000]
mol_size = 20

COUNTER = 0

def build_unique_mol():
    global COUNTER
    mol = qcel.models.Molecule(symbols=["He", "He"], geometry=np.random.rand(2, 3) + COUNTER, validated=True)
    COUNTER += 1
    return mol

if run_tests:
    print("Running tests...\n")
    # Tests
    test_mol1 = build_unique_mol()
    test_mol2 = build_unique_mol()
    test_mol3 = build_unique_mol()

    # Sequential
    ret = storage.add_molecules([test_mol1, test_mol2, test_mol3])["data"]
    assert ret[0] != ret[1]
    assert ret[1] != ret[2]

    # Duplicates
    test_mol1 = build_unique_mol()
    ret = storage.add_molecules([test_mol1, test_mol1, test_mol1])["data"]
    assert len(ret) == 3
    assert ret[0] == ret[1]
    assert ret[1] == ret[2]

    # Duplicates
    test_mol1 = build_unique_mol()
    test_mol2 = build_unique_mol()
    ret = storage.add_molecules([test_mol1, test_mol2, test_mol1])["data"]
    assert len(ret) == 3
    assert ret[0] != ret[1]
    assert ret[0] == ret[2]


print("Running timings...\n")
for trial in mol_trials:
    mols = [build_unique_mol() for x in range(trial)]

    t = time.time()
    ret = storage.add_molecules(mols)["data"]
    ttime = (time.time() - t) * 1000
    time_per_mol = ttime / trial

    print(f"{trial:6d} {ttime:9.3f} {time_per_mol:6.3f}")
