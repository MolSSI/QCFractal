import qcfractal.interface as portal

# Build a interface to the server 
p = portal.QCPortal("localhost:7777")

# Pull data from the server
db = portal.Database("Water", p)

# Submit computations
r = db.query("SCF", "STO-3G", stoich="cp", scale="kcal")

print(db.df)

