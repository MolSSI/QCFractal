import glob
import json

method = "B3LYP"
basis = "aug-cc-pVDZ"

mols = glob.glob("molecules/*json")

def json_geometry(data):
    psimol = "\n  "
    psimol += str(int(data["charge"])) + "  " + str(data["multiplicity"]) + "\n"
    
    for sym, real, mass, xyz in zip(data["symbols"], data["real"], data["masses"], data["geometry"]):
        if real:
            line = " %6s " % sym
        else:
            line = " %6s " % ("GH(" + sym + ")")

        # Ignore masses for now

        line += " % 16.8f   % 16.8f   % 16.8f\n" % tuple(xyz)

        psimol += line
        

    return psimol

for molf in glob.glob("molecules/*json"):

    # Get data
    with open(molf, 'r') as jfile:
        mdata = json.load(jfile)

    mhash = molf.split('/')[1].replace(".json", "") 


    json_data = {}
    json_data["molecule"] = json_geometry(mdata)
    json_data["driver"] = "energy"
    json_data["args"] = method + "/" + basis
    json_data["modelchem"] = method + "/" + basis
    json_data["moleucle_hash"] = mhash 
    #for k,v in json_data.items():
    #    print(k)
    #    print(v)
    #    print("  ")
    #exit()

  
 
    jname = "pages/" + mhash + "_" + method + "_" + basis + ".json"
    with open(jname, 'w') as jfile:
        mdata = json.dump(json_data, jfile, indent=4)

