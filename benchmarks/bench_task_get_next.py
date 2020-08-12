import time
from qcfractal.interface.models.records import ResultRecord
from qcfractal.interface.models import KeywordSet, TaskRecord
import qcfractal
import qcfractal.interface as ptl
import numpy as np
import qcelemental as qcel
import random

print("Building and clearing the database...\n")
db_name = "molecule_tests"
storage = qcfractal.storage_socket_factory(f"postgresql://localhost:5432/{db_name}")
# storage._delete_DB_data(db_name)

INSERTION_QUERY_FLAG = 0

COUNTER_MOL = 0

def create_unique_task(status='WAITING', number=1, num_tags=1, num_programs=1):
    global COUNTER_MOL
    global starting_res_id
    tasks = []

    for i in range(number):
        mol = qcel.models.Molecule(symbols=["He", "He"], geometry=np.random.rand(2, 3) + COUNTER_MOL, validated=True)
        ret = storage.add_molecules([mol])["data"]
        COUNTER_MOL += 1
        result = ResultRecord(version='1', driver='energy', program='games', molecule=ret[0],
                    method='test', basis='6-31g')
        res = storage.add_results([result])["data"]
        tags = ["tag" + str(i + 1) for i in range(num_tags)]
        programs = ["p" + str(i + 1) for i in range(num_programs)]
        program = random.choice(programs)
        tag = random.choice(tags)
        task = ptl.models.TaskRecord(
            **{
                # "hash_index": idx,  # not used anymore
                "spec": {"function": "qcengine.compute_procedure", "args": [{"json_blob": "data"}], "kwargs": {}},
                "tag": tag,
                "program": program,
                "status":status,
                "parser": "",
                "base_result": res[0],
            }
        )
        tasks.append(task)
    return tasks;


if (INSERTION_QUERY_FLAG == 1):

    print ("starting insertion!!!")
    num_tasks = int(1e4)
    number = 2000
    for i in range(1):
        then = time.time()
        tasks = create_unique_task(status='WAITING', number=number, num_tags=2, num_programs=1)
        now = time.time()
        print (f"{number} tasks created in { (now - then) } seconds")
        then = time.time()
        ret = storage.queue_submit(tasks)
        now = time.time()
        print (f"Inserted {len(ret['data'])} tasks in { (now - then) } seconds")

    for i in range(10):
        tasks = create_unique_task(status='ERROR', number=num_tasks, num_tags=3, num_programs=3)
        ret = storage.queue_submit(tasks)
        print (f"Inserted #{i}  {len(ret['data'])} tasks.")
else :
    print ("Running the query!")
    then = time.time()
    storage.queue_get_next(manager=None, available_programs="p1", available_procedures=[], tag=["tag1","tag2"], limit=1000)
    now = time.time()
    print (f"Get time {now - then } second")