import uuid
import json
import subprocess as sp


# Make sure this looks like just a normal file
#runner.run_task.__module__ = "runner"

psi_run = "python /Users/daniel/Gits/psixc/psi4/run_psi4.py --inplace --json "
def psi_compute(json_data):
    return("True")

    filename = str(uuid.uuid4()) + ".json" 
    print(filename)
    
    with open(filename, "w") as outfile:
        json.dump(json_data, outfile)

    print(psi_run + filename)
    sp.call(psi_run + filename, shell=True)    
    
    with open(filename, "w") as outfile:
        ret = json.load(outfile)

    return ret

