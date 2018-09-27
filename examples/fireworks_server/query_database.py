import qcfractal.interface as portal

# Build a interface to the server
p = portal.FractalClient("localhost:7777", verify=False)

# Pull data from the server
db = portal.collections.Dataset.from_server(p, "Water")

# Submit computations
r = db.query("SCF", "STO-3G", stoich="cp", scale="kcal")

# Print the Pandas DataFrame
print(db.df)

# Tests to ensure the correct results are returned
# Safe to comment out
import pytest
pytest.approx(db.df.loc["Water Dimer", "SCF/STO-3G"], 1.e-3) == -1.392710
pytest.approx(db.df.loc["Water Dimer Stretch", "SCF/STO-3G"], 1.e-3) ==  0.037144
pytest.approx(db.df.loc["Helium Dimer", "SCF/STO-3G"], 1.e-3) == -0.003148
