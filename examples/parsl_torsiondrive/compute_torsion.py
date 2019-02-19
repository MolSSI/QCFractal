import qcfractal.interface as portal

# Build a interface to the server 
client = portal.FractalClient("localhost:7777", verify=False)

# Add a HOOH
hooh = portal.data.get_molecule("hooh.json")

# Geometric options
tdinput = {
    "initial_molecule": [hooh],
    "torsiondrive_meta": {
        "dihedrals": [[0, 1, 2, 3]],
        "grid_spacing": [90]
    },
    "optimization_meta": {
        "program": "geometric",
        "coordsys": "tric",
    },
    "qc_meta": {
        "driver": "gradient",
        "method": "UFF",
        "basis": None,
        "options": None,
        "program": "rdkit",
    },
}
# )

# Compute!
ret = client.add_service(tdinput)

print(ret)
