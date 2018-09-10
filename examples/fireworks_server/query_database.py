import qcfractal.interface as portal

# Build a interface to the server 
p = portal.FractalClient("localhost:7777")

# Pull data from the server
db = portal.collections.Database("Water", p)

# Submit computations
r = db.query("SCF", "STO-3G", stoich="cp", scale="kcal")

print(db.df)

