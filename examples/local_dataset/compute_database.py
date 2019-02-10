import qcfractal.interface as portal

# Build a interface to the server 
p = portal.FractalClient("localhost:7777", verify=False)

# Pull data from the server
ds = portal.collections.Dataset.from_server(p, "Water")
print(ds.data)

# Print the current data
# Should be blank, except for an index
print(ds.df)

# Submit computations (cp corrected scf/sto-3g)
r = ds.compute("scf", "sto-3g", stoich="cp", program="psi4")

print("Jobs to be computed:")
print("\n\t-".join([x['molecule'] for x in r.queue]) + "\n")
print("Jobs Already Done:")
print("\n\t-".join([x['molecule'] for x in r.completed]) + "\n")

