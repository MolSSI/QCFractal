import uuid
import json
import subprocess as sp
import os
import time


# Make sure this looks like just a normal file
#runner.run_task.__module__ = "runner"

psi_run = "python /Users/daniel/Gits/psixc/psi4/run_psi4.py --inplace --json "
def psi_compute(json_data):

    filename = str(uuid.uuid4()) + ".json" 
 
    with open(filename, "w") as outfile:
        json.dump(json_data, outfile)

    sp.call(psi_run + filename, shell=True)    
 
    with open(filename, "r") as outfile:
        ret = json.load(outfile)

    os.unlink(filename)

    return ret


if __name__ == "__main__":
    
    json_data = {}
    json_data["molecule"] = """He 0 0 0\n--\nHe 0 0 1"""
    json_data["driver"] = "energy"
    json_data["method"] = 'SCF'
    #json_data["kwargs"] = {"bsse_type": "cp"}
    json_data["options"] = {"BASIS": "STO-3G"}
    json_data["return_output"] = True

    print(psi_compute(json_data))
