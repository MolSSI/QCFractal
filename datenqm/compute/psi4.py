import uuid
import json
import subprocess as sp
import os
import time

# Make sure this looks like just a normal file
#runner.run_task.__module__ = "runner"

try:
    psi_location = os.environ["MONGO_PSI4"]
except:
    raise KeyError("Mongo Compute: MONGO_PSI4 psi variable was not set. Failing.")

psi_run = "python " + psi_location + " --json "


def psi_compute(json_data, **kwargs):

    # Change into the working dir if available
    if "working_dir" in list(json_data):
        os.chdir(json_data["working_dir"])

    # Set memory to 50 GB if not present
    if "memory" not in list(json_data):
        json_data["memory"] = 50e9

    # Set 6 cores
    ncores = kwargs.pop("ncores", 10)

    filename = str(uuid.uuid4()) + ".json"

    with open(filename, "w") as outfile:
        json.dump(json_data, outfile)

    run_script = psi_run + " " + filename
    run_script = psi_run + "-n" + str(ncores) + " " + filename

    output = sp.check_output(run_script, shell=True)

    with open(filename, "r") as outfile:
        ret = json.load(outfile)

    os.unlink(filename)
    ret["subprocess error"] = output

    return ret
