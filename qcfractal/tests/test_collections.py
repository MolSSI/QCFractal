"""
Tests the server collection compute capabilities.
"""

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import fractal_compute_server
import pytest


### Tests an entire server and interaction energy database run
@testing.using_psi4
def test_compute_database(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address(""))
    db_name = "He_PES"
    db = portal.collections.Database(db_name, client, db_type="ie")

    # Adds options
    option = portal.data.get_options("psi_default")

    opt_ret = client.add_options([option])
    opt_key = option["name"]

    # Add two helium dimers to the DB at 4 and 8 bohr
    He1 = portal.Molecule([[2, 0, 0, -2], [2, 0, 0, 2]], dtype="numpy", units="bohr", frags=[1])
    db.add_ie_rxn("He1", He1, attributes={"r": 4}, reaction_results={"default": {"Benchmark": 0.0009608501557}})

    # Save the DB and re-acquire
    r = db.save()
    db = portal.collections.Database.from_server(client, db_name)

    He2 = portal.Molecule([[2, 0, 0, -4], [2, 0, 0, 4]], dtype="numpy", units="bohr", frags=[1])
    db.add_ie_rxn("He2", He2, attributes={"r": 4}, reaction_results={"default": {"Benchmark": -0.00001098794749}})

    # Save the DB and overwrite the result
    r = db.save(overwrite=True)

    # Open a new database
    db = portal.collections.Database.from_server(client, db_name)

    # Compute SCF/sto-3g
    ret = db.compute("SCF", "STO-3G")
    fractal_compute_server.objects["queue_nanny"].await_results()

    # Query computed results
    assert db.query("SCF", "STO-3G")
    assert pytest.approx(0.6024530476071095, 1.e-5) == db.df.ix["He1", "SCF/STO-3G"]
    assert pytest.approx(-0.006895035942673289, 1.e-5) == db.df.ix["He2", "SCF/STO-3G"]

    # Check results
    assert db.query("Benchmark", "", reaction_results=True)
    assert pytest.approx(0.00024477933196125805, 1.e-5) == db.statistics("MUE", "SCF/STO-3G")


### Tests an entire server and interaction energy database run
@testing.using_torsiondrive
@testing.using_geometric
@testing.using_rdkit
def test_compute_biofragment(fractal_compute_server):

    # Obtain a client and build a BioFragment
    client = portal.FractalClient(fractal_compute_server.get_address(""))

    butane = portal.data.get_molecule("butane.json")
    frag = portal.collections.BioFragment("CCCC", butane, client=client)

    # Options
    torsiondrive_options = {
        "torsiondrive_meta": {
            "internal_grid_spacing": [90],
            "terminal_grid_spacing": [90],
        },
        "optimization_meta": {
            "program": "geometric",
            "coordsys": "tric",
        },
        "qc_meta": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "options": "none",
            "program": "rdkit",
        },
    }
    frag.add_options_set("torsiondrive", "v1", torsiondrive_options)

    # Required torsions
    needed_torsions = {
      "internal": [
        [[0, 2, 3, 1]],
      ],
      "terminal": [
        [[3, 2, 0, 4]],
        [[2, 3, 1, 7]],
      ]
    } # yapf: disable

    frag.submit_torsion_drives("v1", needed_torsions)

    # Compute!
    # nanny = fractal_compute_server.objects["queue_nanny"]
    # nanny.await_services(max_iter=5)
    # assert len(nanny.list_current_tasks()) == 0

