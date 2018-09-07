import qcfractal.interface as portal


# Builds a blank database object
# Tell the database we are going to build interaction energies
db = portal.collections.Database("Water", db_type="ie")

# Portal has some molecules stored for easy access.
water_dimer = portal.data.get_molecule("water_dimer_minima.psimol")
water_dimer_stretch = portal.data.get_molecule("water_dimer_stretch.psimol")

# We can also create a new molecule from canonical strings such as a Psi4 molecule
helium_dimer = portal.Molecule("He 0 0 -5\n--\nHe 0 0 5", dtype="psi4")

# Add several intermolecular interaction, dimers are automatically fragmented
db.add_ie_rxn("Water Dimer", water_dimer)
db.add_ie_rxn("Water Dimer Stretch", water_dimer_stretch)
db.add_ie_rxn("Helium Dimer", helium_dimer)

#print(helium_dimer)
#import json
#print(json.dumps(db.data["reactions"][2], indent=2))

# Build a interface to the server 
p = portal.FractalClient("localhost:7777")

# Add the database to the server
db.save(p)

