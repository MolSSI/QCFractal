import qcfractal.interface as portal

# Builds a blank database object
# Tell the database we are going to build interaction energies
ds = portal.collections.ReactionDataset("Water", ds_type="ie")

# Portal has some molecules stored for easy access.
water_dimer = portal.data.get_molecule("water_dimer_minima.psimol")
water_dimer_stretch = portal.data.get_molecule("water_dimer_stretch.psimol")

# We can also create a new molecule from canonical strings such as a Psi4 molecule
helium_dimer = portal.Molecule.from_data("He 0 0 -5\n--\nHe 0 0 5", dtype="psi4")

# Add several intermolecular interaction, dimers are automatically fragmented
ds.add_ie_rxn("Water Dimer", water_dimer)
ds.add_ie_rxn("Water Dimer Stretch", water_dimer_stretch)
ds.add_ie_rxn("Helium Dimer", helium_dimer)

# Build a interface to the server 
p = portal.FractalClient("localhost:7777", verify=False)

# Add the database to the server
ds.save(p)

