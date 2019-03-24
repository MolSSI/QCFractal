import qcfractal.interface as portal

# Build a interface to the server
p = portal.FractalClient("localhost:7777", verify=False)

# Pull data from the server
ds = portal.collections.ReactionDataset.from_server(p, "Water")

# Submit computations
r = ds.query("SCF", "STO-3G", stoich="cp", program="psi4")

# Print the Pandas DataFrame
print(ds.df)

# Tests to ensure the correct results are returned
# Safe to comment out
import pytest
assert pytest.approx(ds.df.loc["Water Dimer", "SCF/STO-3G"], 1.e-3) == -1.392710
assert pytest.approx(ds.df.loc["Water Dimer Stretch", "SCF/STO-3G"], 1.e-3) ==  0.037144
assert pytest.approx(ds.df.loc["Helium Dimer", "SCF/STO-3G"], 1.e-3) == -0.003148

