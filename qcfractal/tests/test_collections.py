"""
Tests the server collection compute capabilities.
"""

import pytest

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import fractal_compute_server


@testing.using_psi4
def test_dataset_compute_gradient(fractal_compute_server):
    client = portal.FractalClient(fractal_compute_server)

    # Build a dataset
    ds = portal.collections.Dataset("ds_energy", client, default_program="psi4", default_driver="gradient")

    local = {"gradient": [0.03, 0, 0.02, -0.02, 0, -0.03]}
    ds.add_entry("He1", portal.Molecule.from_data("He -1 0 0\n--\nHe 0 0 1"), local_results=local)
    ds.add_entry("He2", portal.Molecule.from_data("He -1.1 0 0\n--\nHe 0 0 1.1"), local_results=local)

    # Compute
    ds.save()
    ds.compute("HF", "sto-3g")
    fractal_compute_server.await_results()

    ds.query("HF", "sto-3g", as_array=True)
    ds.query("gradient", None, local_results=True, as_array=True)

    stats = ds.statistics("MUE", "HF/sto-3g", "gradient")
    assert pytest.approx(stats.mean()) == 0.00984176986312362


def test_reactiondataset_check_state(fractal_compute_server):
    client = portal.FractalClient(fractal_compute_server)
    ds = portal.collections.ReactionDataset("check_state", client, ds_type="ie", default_program="rdkit")
    ds.add_ie_rxn("He1", portal.Molecule.from_data("He -3 0 0\n--\nHe 0 0 2"))

    with pytest.raises(ValueError):
        ds.compute("SCF", "STO-3G")

    with pytest.raises(ValueError):
        ds.query("SCF", "STO-3G")

    ds.save()
    assert ds.query("SCF", "STO-3G")

    ds.add_keywords("default", portal.models.KeywordSet(program="psi4", values={"a": 5}))

    with pytest.raises(ValueError):
        ds.query("SCF", "STO-3G")

    ds.save()
    assert ds.query("SCF", "STO-3G")


@testing.using_psi4
def test_compute_reactiondataset_regression(fractal_compute_server):
    """
    Tests an entire server and interaction energy dataset run
    """

    client = portal.FractalClient(fractal_compute_server)
    ds_name = "He_PES"
    ds = portal.collections.ReactionDataset(ds_name, client, ds_type="ie")

    # Add two helium dimers to the DB at 4 and 8 bohr
    He1 = portal.Molecule.from_data([[2, 0, 0, -2], [2, 0, 0, 2]], dtype="numpy", units="bohr", frags=[1])
    ds.add_ie_rxn("He1", He1, attributes={"r": 4}, reaction_results={"default": {"Benchmark": 0.0009608501557}})

    # Save the DB and re-acquire via classmethod
    r = ds.save()
    ds = portal.collections.ReactionDataset.from_server(client, ds_name)
    ds.set_default_program("psi4")
    assert "ReactionDataset(" in str(ds)

    # Test collection lists
    ret = client.list_collections()
    assert ds_name in ret["reactiondataset"]

    ret = client.list_collections("reactiondataset")
    assert ds_name in ret

    He2 = portal.Molecule.from_data([[2, 0, 0, -4], [2, 0, 0, 4]], dtype="numpy", units="bohr", frags=[1])
    ds.add_ie_rxn("He2", He2, attributes={"r": 4}, reaction_results={"default": {"Benchmark": -0.00001098794749}})

    # Save the DB and overwrite the result, reacquire via client
    r = ds.save(overwrite=True)
    ds = client.get_collection("reactiondataset", ds_name)

    # Compute SCF/sto-3g
    ret = ds.compute("SCF", "STO-3G")
    assert len(ret.submitted) == 3
    fractal_compute_server.await_results()

    # Query computed results
    assert ds.query("SCF", "STO-3G")
    assert pytest.approx(0.6024530476071095, 1.e-5) == ds.df.loc["He1", "SCF/STO-3G"]
    assert pytest.approx(-0.006895035942673289, 1.e-5) == ds.df.loc["He2", "SCF/STO-3G"]

    # Check results
    assert ds.query("Benchmark", "", reaction_results=True)
    assert pytest.approx(0.00024477933196125805, 1.e-5) == ds.statistics("MUE", "SCF/STO-3G")

    assert isinstance(ds.to_json(), dict)


@testing.using_psi4
def test_compute_reactiondataset_keywords(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    mol1 = portal.Molecule.from_data("He 0 0 -1.1\n--\nHe 0 0 1.1")

    # Build a dataset
    ds = portal.collections.ReactionDataset("dataset_options", client, ds_type="ie")
    ds.set_default_program("Psi4")

    ds.add_ie_rxn("He2", mol1)
    ds.add_keywords("direct", portal.models.KeywordSet(program="psi4", values={"scf_type": "direct"}), default=True)
    ds.add_keywords("df", portal.models.KeywordSet(program="Psi4", values={"scf_type": "df"}))

    ds.save()

    # Compute, should default to direct options
    r = ds.compute("SCF", "STO-3G")
    fractal_compute_server.await_results()
    assert ds.query("SCF", "STO-3G")
    assert pytest.approx(0.39323818102293856, 1.e-5) == ds.df.loc["He2", "SCF/STO-3G"]

    r = ds.compute("SCF", "STO-3G", keywords="df")
    fractal_compute_server.await_results()
    assert ds.query("SCF", "STO-3G", keywords="df", prefix="df-")
    assert pytest.approx(0.38748602675524185, 1.e-5) == ds.df.loc["He2", "df-SCF/STO-3G"]


@testing.using_torsiondrive
@testing.using_geometric
@testing.using_rdkit
def test_compute_openffworkflow(fractal_compute_server):
    """
    Tests the openffworkflow collection
    """

    # Obtain a client and build a BioFragment
    client = portal.FractalClient(fractal_compute_server)

    openff_workflow_options = {
        # Blank Fragmenter options
        "enumerate_states": {},
        "enumerate_fragments": {},
        "torsiondrive_input": {},

        # TorsionDrive, Geometric, and QC options
        ""
        "torsiondrive_static_options": {
            "keywords": {},
            "optimization_spec": {
                "program": "geometric",
                "keywords": {
                    "coordsys": "tric",
                }
            },
            "qc_spec": {
                "driver": "gradient",
                "method": "UFF",
                "basis": "",
                "keywords": None,
                "program": "rdkit",
            }
        },
        "optimization_static_options": {
            "program": "geometric",
            "keywords": {
                "coordsys": "tric",
            },
            "qc_spec": {
                "driver": "gradient",
                "method": "UFF",
                "basis": "",
                "keywords": None,
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
            "initial_molecule": hooh.json_dict(),
            "grid_spacing": [90],
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
            "initial_molecule": hooh.json_dict(),
            "constraints": {
                'set': [{
                    "type": 'dihedral',
                    "indices": [0, 1, 2, 3],
                    "value": 0
                }]
            }
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
    butane_id = butane.identifiers.canonical_isomeric_explicit_hydrogen_mapped_smiles

    fragment_input = {
        "label1": {
            "type": "torsiondrive_input",
            "initial_molecule": butane.json_dict(),
            "grid_spacing": [90],
            "dihedrals": [[0, 2, 3, 1]],
        },
    }
    wf.add_fragment(butane_id, fragment_input, provenance={})
    assert set(wf.list_fragments()) == {butane_id, "HOOH"}

    final_energies = wf.list_final_energies()
    assert final_energies.keys() == {butane_id, "HOOH"}
    assert final_energies[butane_id].keys() == {"label1"}
    assert final_energies[butane_id]["label1"] == {}


def test_generic_collection(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)
    g = portal.collections.Generic("Generic1", client=client)

    # Check getters/savers
    g["hello"] = 5
    assert g["hello"] == 5

    # Copy the data over
    cp = g.get_data()
    assert cp.data["hello"] == 5
    cp.data["hello"] = 10

    # Make sure we did not change underlying data.
    assert g["hello"] == 5

    # Save semantics slightly wonky, double check this works
    g.save()
    g2 = portal.collections.Generic.from_server(client, "Generic1")
    assert g2["hello"] == 5


def test_missing_collection(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)
    with pytest.raises(KeyError):
        client.get_collection("reactiondataset", "_waffles_")
