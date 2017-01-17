import glob
import json
import subprocess as sp

run_string = "python /home/dsmith/Gits/xcpsi/psi4/run_psi4.py --inplace --json -n6 "

for pagefile in glob.glob("pages/*json"):

    # Get data
    with open(pagefile, 'r') as jfile:
        mdata = json.load(jfile)

    #print("%100s %s" % (pagefile, mdata["success"]))

    if "success" in list(mdata):
        continue

    print("Computing pagefile %s" % pagefile)

    sp.call(run_string + pagefile, shell=True) 

