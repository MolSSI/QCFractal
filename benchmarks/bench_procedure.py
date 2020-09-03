import time
from qcfractal.interface.models.records import ResultRecord
from qcfractal.interface.models import KeywordSet
import qcfractal
import qcfractal.interface as ptl
import numpy as np
import qcelemental as qcel
import random

print("Building and clearing the database...\n")
db_name = "molecule_tests"
storage = qcfractal.storage_socket_factory(f"postgresql://localhost:5432/{db_name}")
storage._delete_DB_data(db_name)

def build_unique_mol(num = 1):
    mols = [qcel.models.Molecule(symbols=["He", "He"], geometry=np.random.rand(2, 3) + i, validated=True)
                for i in range(num)]
    ret = storage.add_molecules(mols)["data"]
    return ret


def create_unique_result(mols):
    results = [ResultRecord(version='1', driver='energy', program='games', molecule=mol_ret,
                    method='test', basis='6-31g') for mol_ret in mols]
    ret = storage.add_results(results)["data"]
    return ret

def create_procedures(res_ids, mol_ids, num=1):
    methods = ["HF", "uff ", "pm6", "hf3c"]
    basis = ["dzvp", "6-31g", "dzvp", "sto-3g", None]
    programs = ["psi4", "rdkit", "mopac", "ddk"]

    record_list = []
    for i in range(num):
        proc_template = {
        "procedure": "optimization",
        "initial_molecule": mol_ids[i],
        "program": "something",
        "hash_index": None,
        # "trajectory": None,
        "trajectory": [res_ids[i]],
        "qc_spec": {
            "driver": "gradient",
            "method": random.choice(methods),
            "basis": random.choice(basis),
            # "keywords": None,
            "program": random.choice(programs),
            },
        }
        record_list.append(ptl.models.OptimizationRecord(**proc_template))
    ret = storage.add_procedures(record_list=record_list)["data"]
    
    return ret

proc_trials = [1, 10, 100, 500, 1000]

def main():
    print("Running timings for add and update...\n")
for trial in proc_trials:
    # adding procedures and benchmarking
    mol_ids = build_unique_mol(num=trial)
    res_ids = create_unique_result(mol_ids)

    t = time.time()
    proc_ids = create_procedures(res_ids, mol_ids, num=trial)
    
    ttime = (time.time() - t) * 1000
    time_per_proc = ttime / trial

    print(f"add   : {trial:6d} {ttime:9.3f} {time_per_proc:6.3f}")
    
    # get_procedure benchmark section
    t = time.time()
    ret = storage.get_procedures(procedure="optimization", status=None)

    ttime = (time.time() - t) * 1000
    time_per_proc = ttime / trial

    print(f"get   : {trial:6d} {ttime:9.3f} {time_per_proc:6.3f}")