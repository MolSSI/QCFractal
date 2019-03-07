import qcfractal.interface as portal

# Build a interface to the server 
client = portal.FractalClient("localhost:7777", verify=False)

# Add a HOOH
hooh = portal.data.get_molecule("hooh.json")

# Geometric options
tdinput = {
    "initial_molecule": [hooh],
    "keywords": {
        "dihedrals": [[0, 1, 2, 3]],
        "grid_spacing": [90]
    },
    "optimization_spec": {
        "program": "geometric",
        "keywords": {
            "coordsys": "tric",
        }
    },
    "qc_spec": {
        "driver": "gradient",
        "method": "UFF",
        "basis": None,
        "keywords": None,
        "program": "rdkit",
    },
}

# Compute!
ret = client.add_service([tdinput])

print(ret)
