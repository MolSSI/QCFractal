import time
from qcfractal.interface.models.records import ResultRecord
from qcfractal.interface.models import KeywordSet, TaskRecord
import qcfractal
import qcfractal.interface as ptl
import numpy as np
import qcelemental as qcel

print("Building and clearing the database...\n")
db_name = "molecule_tests"
storage = qcfractal.storage_socket_factory(f"postgresql://localhost:5432/{db_name}")
storage._delete_DB_data(db_name)

run_tests = True
mol_trials = [1, 5, 10, 25, 50, 100, 500, 1000]

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

def create_unique_task():

    result = create_unique_result()
    res = storage.add_results([result])["data"]
    task = ptl.models.TaskRecord(
        **{
            # "hash_index": idx,  # not used anymore
            "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
            "tag": None,
            "program": "p1",
            "parser": "",
            "base_result": res[0],
        }
    )
    return task;

if run_tests:
    print("Running tests...\n")
    # Tests
    test_task1 = create_unique_task()
    test_task2 = create_unique_task()
    test_task3 = create_unique_task()

    # Sequential
    ret = storage.queue_submit([test_task1, test_task2, test_task3])["data"]
    assert ret[0] != ret[1]
    assert ret[1] != ret[2]

    # Multiple inserts
    ret1 = storage.queue_submit([test_task1, test_task2, test_task3])["data"]
    ret2 = storage.queue_submit([test_task1, test_task2, test_task3])["data"]
    assert ret1[0] == ret2[0]
    assert ret1[1] == ret2[1]
    assert ret1[2] == ret2[2]

    print("Running timings for add and update...\n")
    for trial in mol_trials:
        tasks = [create_unique_task() for x in range(trial)]


        t = time.time()
        ret = storage.queue_submit(tasks)["data"]

        ttime = (time.time() - t) * 1000
        time_per_mol = ttime / trial

        print(f"add   : {trial:6d} {ttime:9.3f} {time_per_mol:6.3f}")

        for r, rid in zip(tasks, ret):
            r.__dict__["id"] = rid

        print()
