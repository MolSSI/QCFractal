import qcfractal.interface as portal
import json

# Build a interface to the server 
p = portal.FractalClient("localhost:7777")

# Add "default" options
option = portal.data.get_options("psi_default")
opt_ret = p.add_options([option])

# Pull data from the server
db = portal.collections.Database("Water", p)

# Print the current data
# Should be blank, except for an index
print(db.df)

# Submit computations (cp corrected scf/sto-3g)
r = db.compute("scf", "sto-3g", stoich="cp")

print("Jobs to be computed")
print(json.dumps(r, indent=2))

