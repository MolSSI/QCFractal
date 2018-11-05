"""
Tests the server collection compute capabilities.
"""

import pytest

import qcfractal.interface as portal
from qcfractal import testing
# Only use dask
from qcfractal.testing import dask_server_fixture as fractal_compute_server


### Tests an entire server and interaction energy dataset run
@testing.using_psi4
def test_compute_dataset(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address(""))
    ds_name = "He_PES"
    ds = portal.collections.Dataset(ds_name, client, ds_type="ie")

    # Adds options
    option = portal.data.get_options("psi_default")

    opt_ret = client.add_options([option])
    opt_key = option["name"]

    # Add two helium dimers to the DB at 4 and 8 bohr
    He1 = portal.Molecule([[2, 0, 0, -2], [2, 0, 0, 2]], dtype="numpy", units="bohr", frags=[1])
    ds.add_ie_rxn("He1", He1, attributes={"r": 4}, reaction_results={"default": {"Benchmark": 0.0009608501557}})

    # Save the DB and re-acquire via classmethod
    r = ds.save()
    ds = portal.collections.Dataset.from_server(client, ds_name)
    assert "Dataset(" in str(ds)

    # Test collection lists
    ret = client.list_collections()
    assert ret == {"dataset": [ds_name]}

    ret = client.list_collections("dataset")
    assert ret == [ds_name]

    He2 = portal.Molecule([[2, 0, 0, -4], [2, 0, 0, 4]], dtype="numpy", units="bohr", frags=[1])
    ds.add_ie_rxn("He2", He2, attributes={"r": 4}, reaction_results={"default": {"Benchmark": -0.00001098794749}})

    # Save the DB and overwrite the result, reacquire via client
    r = ds.save(overwrite=True)
    ds = client.get_collection("dataset", ds_name)

    # Compute SCF/sto-3g
    ret = ds.compute("SCF", "STO-3G")
    assert len(ret["submitted"]) == 3
    fractal_compute_server.await_results()

    # Query computed results
    assert ds.query("SCF", "STO-3G")
    assert pytest.approx(0.6024530476071095, 1.e-5) == ds.df.loc["He1", "SCF/STO-3G"]
    assert pytest.approx(-0.006895035942673289, 1.e-5) == ds.df.loc["He2", "SCF/STO-3G"]

    # Check results
    assert ds.query("Benchmark", "", reaction_results=True)
    assert pytest.approx(0.00024477933196125805, 1.e-5) == ds.statistics("MUE", "SCF/STO-3G")

    assert isinstance(ds.to_json(), dict)

### Tests the biofragment collection
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

    # frag.submit_torsion_drives("v1", needed_torsions)


### Tests the openffworkflow collection
@testing.using_torsiondrive
@testing.using_geometric
@testing.using_rdkit
def test_compute_openffworkflow(fractal_compute_server):

    # Obtain a client and build a BioFragment
    client = portal.FractalClient(fractal_compute_server.get_address(""))

    openff_workflow_options = {
        # Blank Fragmenter options
        "enumerate_states": {},
        "enumerate_fragments": {},
        "torsiondrive_input": {},

        # TorsionDrive, Geometric, and QC options
        ""
        "torsiondrive_static_options":{
          "torsiondrive_meta": {},
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
          }
        },
        "optimization_static_options": {
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
            }
        }
    }
    wf = portal.collections.OpenFFWorkflow("Workflow1", client=client, **openff_workflow_options)

    # # Add a fragment and wait for the compute
    hooh = portal.data.get_molecule("hooh.json")
    fragment_input = {
        "label1": {
            "type": "torsiondrive_input",
            "initial_molecule": hooh.to_json(),
            "grid_spacing": [120],
            "dihedrals": [[0, 1, 2, 3]],
        },
    }
    wf.add_fragment("HOOH", fragment_input, provenance={})
    assert set(wf.list_fragments()) == {"HOOH"}
    fractal_compute_server.await_services(max_iter=5)

    final_energies = wf.list_final_energies()
    assert final_energies.keys() == {"HOOH"}
    assert final_energies["HOOH"].keys() == {"label1"}

    final_molecules = wf.list_final_molecules()
    assert final_molecules.keys() == {"HOOH"}
    assert final_molecules["HOOH"].keys() == {"label1"}

    optimization_input = {
        "label2": {
            "type": "optimization_input",
            "initial_molecule": hooh.to_json(),
            "constraints": {'set': [{"type": 'dihedral', "indices": [0, 1, 2, 3], "value": 0}]}
        }
    }

    wf.add_fragment("HOOH", optimization_input, provenance={})
    fractal_compute_server.await_services(max_iter=5)

    final_energies = wf.list_final_energies()
    assert final_energies["HOOH"].keys() == {"label1", "label2"}
    assert pytest.approx(0.00259754, 1.e-4) == final_energies["HOOH"]["label2"]

    final_molecules = wf.list_final_molecules()
    assert final_molecules.keys() == {"HOOH"}
    assert final_molecules["HOOH"].keys() == {"label1", "label2"}

    # Add a second fragment
    butane = portal.data.get_molecule("butane.json")
    butane_id = butane.identifiers["canonical_isomeric_explicit_hydrogen_mapped_smiles"]

    fragment_input = {
        "label1": {
            "type": "torsiondrive_input",
            "initial_molecule": butane.to_json(),
            "grid_spacing": [90],
            "dihedrals": [[0, 2, 3, 1]],
        },
    }
    wf.add_fragment(butane_id, fragment_input, provenance={})
    assert set(wf.list_fragments()) == {butane_id, "HOOH"}

    final_energies = wf.list_final_energies()
    assert final_energies.keys() == {butane_id, "HOOH"}
    assert final_energies[butane_id].keys() == {"label1"}
    assert final_energies[butane_id]["label1"] is None





