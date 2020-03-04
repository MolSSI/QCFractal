import time
from qcfractal.interface.models.records import ResultRecord
import qcfractal
import numpy as np
import qcelemental as qcel

print("Building and clearing the database...\n")
db_name = "molecule_tests"
storage = qcfractal.storage_socket_factory(f"postgresql://localhost:5432/{db_name}")
storage._delete_DB_data(db_name)

run_tests = False
mol_trials = [1, 5, 10, 25, 50, 100, 500, 1000]
# _size = 20

COUNTER_MOL = 0


def build_unique_mol():
    global COUNTER_MOL
    mol = qcel.models.Molecule(symbols=["He", "He"], geometry=np.random.rand(2, 3) + COUNTER_MOL, validated=True)
    COUNTER_MOL += 1
    return mol


def create_unique_result():
    mol = build_unique_mol()
    ret = storage.add_molecules([mol])["data"]
    result = ResultRecord(version='1', driver='energy', program='games', molecule=ret[0],
                          method='test', basis='6-31g')

    return result


if run_tests:
    print("Running tests...\n")
    # Tests
    test_res1 = create_unique_result()
    test_res2 = create_unique_result()
    test_res3 = create_unique_result()

    # Sequential
    ret = storage.add_results([test_res1, test_res2, test_res3])["data"]
    assert ret[0] != ret[1]
    assert ret[1] != ret[2]

    # Duplicates
    test_mol1 = create_unique_result()
    ret = storage.add_results([test_res1, test_res2, test_res3])["data"]
    assert len(ret) == 3
    assert ret[0] == ret[1]
    assert ret[1] == ret[2]

    # Duplicates
    test_res1 = create_unique_result()
    test_res2 = create_unique_result()
    ret = storage.add_results([test_res1, test_res2, test_res1])["data"]
    assert len(ret) == 3
    assert ret[0] != ret[1]
    assert ret[0] == ret[2]


print("Running timings for add and update...\n")
for trial in mol_trials:
    results = [create_unique_result() for x in range(trial)]


    t = time.time()
    ret = storage.add_results(results)["data"]

    ttime = (time.time() - t) * 1000
    time_per_mol = ttime / trial

    print(f"add   : {trial:6d} {ttime:9.3f} {time_per_mol:6.3f}")
    
    for r, rid in zip(results, ret):
        r.__dict__["id"] = rid

    t = time.time()
    ret = storage.update_results(results)
    ttime = (time.time() - t) * 1000
    time_per_mol = ttime / trial
    trial = len(results)

    print(f"update: {trial:6d} {ttime:9.3f} {time_per_mol:6.3f}")
    print()

