"""
Tests the server collection compute capabilities.
"""

import numpy as np
import pytest

import qcelemental as qcel
import qcfractal.interface as ptl
from qcfractal import testing
from qcfractal.testing import fractal_compute_server


def test_collection_query(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    ds = ptl.collections.Dataset("CAPITAL", client)
    ds.save()

    cols = client.list_collections()
    assert ("Dataset", "CAPITAL") in cols.index

    ds = client.get_collection("dataset", "capital")
    assert ds.name == "CAPITAL"

    ds = client.get_collection("DATAset", "CAPital")
    assert ds.name == "CAPITAL"


@pytest.fixture(scope="module")
def gradient_dataset_fixture(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    testing.check_has_module("psi4")

    # Build a dataset
    ds = ptl.collections.Dataset("ds_gradient",
                                 client,
                                 default_program="psi4",
                                 default_driver="gradient",
                                 default_units="hartree/bohr")

    ds.add_entry("He1", ptl.Molecule.from_data("He -1 0 0\n--\nHe 0 0 1"))
    ds.add_entry("He2", ptl.Molecule.from_data("He -1.1 0 0\n--\nHe 0 0 1.1"))
    ds.save()

    contrib = {
        "name": "Gradient",
        "theory_level": "pseudo-random values",
        "values": {
            "He1": [0.03, 0, 0.02, -0.02, 0, -0.03],
            "He2": [0.03, 0, 0.02, -0.02, 0, -0.03]
        },
        "theory_level_details": {
            "driver": "gradient"
        },
        "units": "hartree/bohr"
    }
    contrib_no_details = {
        "name": "no details",
        "theory_level": "pseudo-random values",
        "values": {
            "He1": [0.03, 0, 0.02, -0.02, 0, -0.03],
            "He2": [0.03, 0, 0.02, -0.02, 0, -0.03]
        },
        "units": "hartree/bohr"
    }
    contrib_all_details = {
        "name": "all details",
        "theory_level": "pseudo-random values",
        "values": {
            "He1": [0.03, 0, 0.02, -0.02, 0, -0.03],
            "He2": [0.03, 0, 0.02, -0.02, 0, -0.03]
        },
        "theory_level_details": {
            "driver": "gradient",
            "program": "fake_program",
            "basis": "fake_basis",
            "method": "fake_method",
            "keywords": "fake_keywords"
        },
        "units": "hartree/bohr"
    }
    ds.add_contributed_values(contrib)
    ds.add_contributed_values(contrib_no_details)
    ds.add_contributed_values(contrib_all_details)

    ds.add_keywords("scf_default", "psi4", ptl.models.KeywordSet(values={}), default=True)
    ds.save()

    ds.compute("HF", "sto-3g")
    fractal_compute_server.await_results()

    assert ds.get_records("HF", "sto-3g").iloc[0, 0].status == "COMPLETE"
    assert ds.get_records("HF", "sto-3g").iloc[1, 0].status == "COMPLETE"

    yield client, client.get_collection("dataset", "ds_gradient")


def test_gradient_dataset_get_molecules(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    he1_dist = 2.672476322216822
    he2_dist = 2.939723950195864
    mols = ds.get_molecules()
    assert mols.shape == (2, 1)
    assert mols.iloc[0, 0].measure([0, 1]) == pytest.approx(he1_dist)

    mol = ds.get_molecules(subset="He1")
    assert mol.measure([0, 1]) == pytest.approx(he1_dist)

    mol = ds.get_molecules(subset="He2")
    assert mol.measure([0, 1]) == pytest.approx(he2_dist)

    mols_subset = ds.get_molecules(subset=["He1"])
    assert mols_subset.iloc[0, 0].measure([0, 1]) == pytest.approx(he1_dist)

    with pytest.raises(KeyError):
        ds.get_molecules(subset="NotInDataset")


def test_gradient_dataset_get_records(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    records = ds.get_records("HF", "sto-3g")
    assert records.shape == (2, 1)
    assert records.iloc[0, 0].status == "COMPLETE"
    assert records.iloc[1, 0].status == "COMPLETE"

    records_subset1 = ds.get_records("HF", "sto-3g", subset="He2")
    assert records_subset1.status == "COMPLETE"

    records_subset2 = ds.get_records("HF", "sto-3g", subset=["He2"])
    assert records_subset2.shape == (1, 1)
    assert records_subset2.iloc[0, 0].status == "COMPLETE"

    rec_proj = ds.get_records("HF", "sto-3g", projection={"extras": True, "return_result": True})
    assert rec_proj.shape == (2, 2)
    assert set(rec_proj.columns) == {"extras", "return_result"}

    with pytest.raises(KeyError):
        ds.get_records(method="NotInDataset")


def test_gradient_dataset_get_values(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    cols = set(ds.get_values().columns)
    names = set(ds.list_values().reset_index()['name'])
    assert cols == names

    df = ds.get_values()
    assert df.shape == (len(ds.get_index()), 4)
    for entry in ds.get_index():
        assert (df.loc[entry, "HF/sto-3g"] == ds.get_records(subset=entry,
                                                             method="hf",
                                                             basis="sto-3g",
                                                             projection={'return_result': True})).all()

    assert ds.get_values(method="NotInDataset").shape[1] == 0  # 0-length DFs can cause exceptions

    with pytest.warns(RuntimeWarning):
        ds.get_values(name="HF/sto-3g", basis="sto-3g")


def test_gradient_dataset_list_values(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    # List contributed values
    df = ds.list_values(native=False).reset_index()
    assert df.shape == (3, 7)
    assert set(df.columns) == {*ds.data.history_keys, "name", "native"}
    assert {x.lower() for x in df['name']} == set(ds.data.contributed_values.keys())

    df = ds.list_values(driver="graDieNt", native=False).reset_index()
    assert {x.lower() for x in df['name']} == {"gradient", "all details"}

    names = ["GrAdIeNt", "no details"]
    df = ds.list_values(name=names, native=False).reset_index()
    assert {x.lower() for x in df['name']} == {name.lower() for name in names}

    # List native values
    df1 = ds.list_values(native=True).reset_index()
    assert df1.shape == (1, 7)
    assert set(df1.columns) == {*ds.data.history_keys, "name", "native"}

    df2 = ds.list_values(method='hf', basis='sto-3g', native=True).reset_index()
    df3 = ds.list_values(name='hf/sto-3g', native=True).reset_index()
    assert (df1 == df2).all().all()
    assert (df1 == df3).all().all()

    # All values
    df = ds.list_values().reset_index()
    assert df.shape == (4, 7)
    assert set(df.columns) == {*ds.data.history_keys, "name", "native"}

    df = ds.list_values(driver="gradient").reset_index()
    assert df.shape == (3, 7)

    df = ds.list_values(name="Not in dataset").reset_index()
    assert len(df) == 0


def test_gradient_dataset_statistics(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    df = ds.get_values(native=True)
    assert df.shape == (2, 1)
    assert np.sum(df.loc["He2", "HF/sto-3g"]) == pytest.approx(0.0)

    # Test out some statistics
    stats = ds.statistics("MUE", "HF/sto-3g", "Gradient")
    assert pytest.approx(stats.mean(), 1.e-5) == 0.00984176986312362

    stats = ds.statistics("UE", "HF/sto-3g", "Gradient")
    assert pytest.approx(stats.loc["He1"].mean(), 1.e-5) == 0.01635020639
    assert pytest.approx(stats.loc["He2"].mean(), 1.e-5) == 0.00333333333


@pytest.fixture(scope="module")
def contributed_dataset_fixture(fractal_compute_server):
    """ Fixture for testing rich contributed datasets with many properties and molecules of different sizes"""
    client = ptl.FractalClient(fractal_compute_server)

    testing.check_has_module("psi4")

    # Build a dataset
    ds = ptl.collections.Dataset("ds_contributed", client)

    ds.add_entry("He1", ptl.Molecule.from_data("He -1 0 0\n--\nHe 0 0 1"))
    ds.add_entry("He", ptl.Molecule.from_data("He -1.1 0 0"))
    ds.units = "hartree"
    ds.save()

    energy = {
        "name": "Fake Energy",
        "theory_level": "pseudo-random values",
        "values": {
            "He1": 1234.5,
            "He": 5.4321
        },
        "theory_level_details": {
            "driver": "energy",
            "program": "fake_program",
            "basis": "fake_basis",
            "method": "fake_method",
        },
        "units": "hartree"
    }
    gradient = {
        "name": "Fake Gradient",
        "theory_level": "pseudo-random values",
        "values": {
            "He1": [0.03, 0, 0.02, -0.02, 0, -0.03],
            "He": [0.03, 0, 0.02]
        },
        "theory_level_details": {
            "driver": "gradient",
            "program": "fake_program",
            "basis": "fake_basis",
            "method": "fake_method",
        },
        "units": "hartree/bohr"
    }
    hessian = {
        "name": "Fake Hessian",
        "theory_level": "pseudo-random values",
        "values": {
            "He1": list(np.eye(6).ravel()),
            "He": [1, 0.2, 0.1, 0.2, 1, 0.4, 0.1, 0.4, 1]
        },
        "theory_level_details": {
            "driver": "hessian",
            "program": "fake_program",
            "basis": "fake_basis",
            "method": "fake_method",
        },
        "units": "hartree/bohr**2"
    }
    dipole = {
        "name": "Fake Dipole",
        "theory_level": "pseudo-random values",
        "values": {
            "He": [1., 2., 3.],
            "He1": [1., -2., 0.]
        },
        "theory_level_details": {
            "driver": "dipole",
            "program": "fake_program",
            "basis": "fake_basis",
            "method": "fake_method",
        },
        "units": "e * bohr"
    }
    energy_FF = {
        "name": "Fake FF Energy",
        "theory_level": "some force field",
        "values": {
            "He1": 0.5,
            "He": 42.
        },
        "theory_level_details": {
            "driver": "energy",
            "program": "fake_program",
            "basis": None,
            "method": "fake_method"
        },
        "units": "hartree"
    }

    ds.add_contributed_values(energy)
    ds.add_contributed_values(gradient)
    ds.add_contributed_values(hessian)
    ds.add_contributed_values(dipole)
    ds.add_contributed_values(energy_FF)

    with pytest.raises(KeyError):
        ds.add_contributed_values(energy)

    ds.save()

    yield client, client.get_collection("dataset", "ds_contributed")


def test_dataset_contributed_units(contributed_dataset_fixture):
    _, ds = contributed_dataset_fixture

    assert qcel.constants.ureg(ds._column_metadata[ds.get_values(
        name="Fake Energy").columns[0]]["units"]) == qcel.constants.ureg("kcal / mol")
    assert qcel.constants.ureg(ds._column_metadata[ds.get_values(
        name="Fake Gradient").columns[0]]["units"]) == qcel.constants.ureg("hartree/bohr")
    assert qcel.constants.ureg(ds._column_metadata[ds.get_values(
        name="Fake Hessian").columns[0]]["units"]) == qcel.constants.ureg("hartree/bohr**2")
    assert qcel.constants.ureg(
        ds._column_metadata[ds.get_values(name="Fake Dipole").columns[0]]["units"]) == qcel.constants.ureg("e * bohr")


def test_dataset_contributed_mixed_values(contributed_dataset_fixture):
    _, ds = contributed_dataset_fixture

    unselected_values = ds.get_values()
    assert unselected_values.shape == (2, 5)
    selected_values = ds.get_values(program='fake_program')
    assert selected_values.shape == (2, 5)
    selected_values = ds.get_values(basis="None")
    assert selected_values.shape == (2, 1)
    assert selected_values.columns[0] == "Fake FF Energy"


def test_dataset_compute_response(fractal_compute_server):
    """ Tests that the full compute response is returned when calling Dataset.compute """
    client = ptl.FractalClient(fractal_compute_server)

    # Build a dataset
    ds = ptl.collections.Dataset("ds",
                                 client,
                                 default_program="psi4",
                                 default_driver="energy",
                                 default_units="hartree")

    ds.add_entry("He1", ptl.Molecule.from_data("He -1 0 0\n--\nHe 0 0 1"))
    ds.add_entry("He2", ptl.Molecule.from_data("He -1.1 0 0\n--\nHe 0 0 1.1"))

    ds.save()

    # Compute fewer molecules than query limit
    response = ds.compute("HF", "sto-3g")
    assert len(response.ids) == 2

    # Compute more molecules than query limit
    client.query_limit = 1
    response = ds.compute("HF", "sto-3g")
    assert len(response.ids) == 2


def test_reactiondataset_check_state(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)
    ds = ptl.collections.ReactionDataset("check_state", client, ds_type="ie", default_program="rdkit")
    ds.add_ie_rxn("He1", ptl.Molecule.from_data("He -3 0 0\n--\nNe 0 0 2"))

    with pytest.raises(ValueError):
        ds.compute("SCF", "STO-3G")

    with pytest.raises(ValueError):
        ds.get_records("SCF", "STO-3G")

    ds.save()
    ds.get_records("SCF", "STO-3G")

    ds.add_keywords("default", "psi4", ptl.models.KeywordSet(values={"a": 5}))

    with pytest.raises(ValueError):
        ds.get_records("SCF", "STO-3G")

    ds.save()

    ds.get_records("SCF", "STO-3G")

    contrib = {
        "name": "Benchmark",
        "doi": None,
        "theory_level": "very high",
        "values": {
            "He1": 0.0009608501557,
            "He2": -0.00001098794749
        },
        "units": "hartree"
    }
    # No entry for He2 in the dataset
    with pytest.raises(ValueError):
        ds.add_contributed_values(contrib)

    ds.add_ie_rxn("He2", ptl.Molecule.from_data("He -2 0 0\n--\nNe 0 0 2"))
    ds.save()

    ds.add_contributed_values(contrib)

    with pytest.raises(ValueError):
        ds.get_records("SCF", "STO-3G")

    assert "benchmark" == ds.list_values(native=False).reset_index()["name"][0].lower()
    ds.units = "hartree"
    bench = ds.get_values(name="benchmark", native=False)
    assert ds._column_metadata[bench.columns[0]]["native"] is False
    assert bench.shape == (2, 1)
    assert bench.loc["He1"][0] == contrib["values"]["He1"]
    assert bench.loc["He2"][0] == contrib["values"]["He2"]


@pytest.fixture(scope="module")
def reactiondataset_dftd3_fixture_fixture(fractal_compute_server):

    testing.check_has_module("psi4")
    testing.check_has_module("dftd3")

    client = ptl.FractalClient(fractal_compute_server)
    ds_name = "He_DFTD3"
    ds = ptl.collections.ReactionDataset(ds_name, client, ds_type="ie")

    # Add two helium dimers to the DB at 4 and 8 bohr
    HeDimer = ptl.Molecule.from_data([[2, 0, 0, -4.123], [2, 0, 0, 4.123]], dtype="numpy", units="bohr", frags=[1])
    ds.add_ie_rxn("HeDimer", HeDimer, attributes={"r": 4})
    ds.set_default_program("psi4")
    ds.add_keywords("scf_default", "psi4", ptl.models.KeywordSet(values={}), default=True)

    ds.save()

    ncomp1 = ds.compute("B3LYP-D3", "6-31g")
    assert len(ncomp1.ids) == 4
    assert len(ncomp1.submitted) == 4

    ncomp2 = ds.compute("B3LYP-D3(BJ)", "6-31g")
    assert len(ncomp2.ids) == 4
    assert len(ncomp2.submitted) == 2

    fractal_compute_server.await_results()

    yield client, ds


def test_rectiondataset_dftd3_records(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture

    records = ds.get_records("B3LYP", "6-31g")
    assert records.shape == (1, 1)
    assert records.iloc[0, 0].status == "COMPLETE"

    records = ds.get_records("B3LYP", "6-31g", stoich=["cp", "default"])
    assert records.shape == (2, 1)
    assert records.iloc[0, 0].status == "COMPLETE"
    assert records.iloc[0, 0].id == records.iloc[1, 0].id

    records = ds.get_records("B3LYP", "6-31g", stoich=["cp", "default"], subset="HeDimer")
    assert records.shape == (2, 1)

    # No molecules
    with pytest.raises(KeyError):
        records = ds.get_records("B3LYP", "6-31g", stoich=["cp", "default"], subset="Gibberish")


def test_rectiondataset_dftd3_energies(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture

    bench = {
        "B3LYP/6-31g": pytest.approx(-0.002135, 1.e-3),
        "B3LYP-D3/6-31g": pytest.approx(-0.005818, 1.e-3),
        "B3LYP-D3(BJ)/6-31g": pytest.approx(-0.005636, 1.e-3)
    }

    ret = ds.get_values("B3LYP", "6-31G")
    assert ret.loc["HeDimer", "B3LYP/6-31g"] == bench["B3LYP/6-31g"]

    ret = ds.get_values("B3LYP-D3", "6-31G")
    assert ret.loc["HeDimer", "B3LYP-D3/6-31g"] == bench["B3LYP-D3/6-31g"]

    ret = ds.get_values("B3LYP-D3(BJ)", "6-31G")
    assert ret.loc["HeDimer", "B3LYP-D3(BJ)/6-31g"] == bench["B3LYP-D3(BJ)/6-31g"]

    # Should be in ds.df now as wells
    for key, value in bench.items():
        assert value == ds.df.loc["HeDimer", key]


def test_rectiondataset_dftd3_molecules(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture

    mols = ds.get_molecules()
    assert mols.shape == (1, 1)
    assert np.all(mols.iloc[0, 0].real)  # Should be all real
    assert tuple(mols.index) == (("HeDimer", "default", 0), )

    mols = ds.get_molecules(stoich="cp1")
    assert mols.shape == (1, 1)
    assert not np.all(mols.iloc[0, 0].real)  # Should be half real
    assert tuple(mols.index) == (("HeDimer", "cp1", 0), )

    stoichs = ["cp1", "default1", "cp", "default"]
    mols = ds.get_molecules(stoich=stoichs)
    assert mols.shape == (4, 1)

    mols = mols.reset_index()
    assert set(stoichs) == set(mols["stoichiometry"])


def test_dataset_dftd3(reactiondataset_dftd3_fixture_fixture):
    client, rxn_ds = reactiondataset_dftd3_fixture_fixture

    ds_name = "He_DFTD3"
    ds = ptl.collections.Dataset(ds_name, client)

    HeDimer = rxn_ds.get_molecules(subset='HeDimer').iloc[0, 0]
    ds.add_entry("HeDimer", HeDimer)
    ds.set_default_program(rxn_ds.data.default_program)
    ds.add_keywords("scf_default", rxn_ds.data.default_program, ptl.models.KeywordSet(values={}), default=True)

    ds.save()

    ds.compute("B3LYP-D3", "6-31g")

    d3 = ds.get_values(method="b3lyp-d3")
    assert d3.shape == (1, 1)
    b3lyp = ds.get_values(method="b3lyp")
    assert b3lyp.shape == (1, 1)

    assert d3.iloc[0, 0] != b3lyp.iloc[0, 0]


@testing.using_psi4
def test_compute_reactiondataset_regression(fractal_compute_server):
    """
    Tests an entire server and interaction energy dataset run
    """

    client = ptl.FractalClient(fractal_compute_server)
    ds_name = "He_PES"
    ds = ptl.collections.ReactionDataset(ds_name, client, ds_type="ie")

    # Add two helium dimers to the DB at 4 and 8 bohr
    He1 = ptl.Molecule.from_data([[2, 0, 0, -2], [2, 0, 0, 2]], dtype="numpy", units="bohr", frags=[1])
    ds.add_ie_rxn("He1", He1, attributes={"r": 4})

    # Save the DB and re-acquire via classmethod
    r = ds.save()
    ds = ptl.collections.ReactionDataset.from_server(client, ds_name)
    ds.set_default_program("psi4")
    assert "ReactionDataset(" in str(ds)

    # Test collection lists
    ret = client.list_collections(aslist=True)
    assert ds_name in ret["ReactionDataset"]

    ret = client.list_collections("reactiondataset", aslist=True)
    assert ds_name in ret

    He2 = ptl.Molecule.from_data([[2, 0, 0, -4], [2, 0, 0, 4]], dtype="numpy", units="bohr", frags=[1])
    ds.add_ie_rxn("He2", He2, attributes={"r": 4})
    ds.save()

    contrib = {
        "name": "Benchmark",
        "doi": None,
        "theory_level": "very high",
        "values": {
            "He1": 0.0009608501557,
            "He2": -0.00001098794749
        },
        "units": "hartree"
    }
    ds.add_contributed_values(contrib)
    ds.set_default_benchmark("Benchmark")

    # Save the DB and overwrite the result, reacquire via client
    ds.save()
    ds = client.get_collection("reactiondataset", ds_name)

    with pytest.raises(KeyError):
        ds.compute("SCF", "STO-3G", stoich="nocp")  # Should be 'default' not 'nocp'

    # Compute SCF/sto-3g
    ret = ds.compute("SCF", "STO-3G")
    assert len(ret.submitted) == 3
    fractal_compute_server.await_results()

    # Query computed results
    ret = ds.get_values("SCF", "STO-3G")
    assert ret.shape == (2, 1)
    assert pytest.approx(0.6024530476, 1.e-5) == ret.loc["He1", "SCF/sto-3g"]
    assert pytest.approx(-0.0068950359, 1.e-5) == ret.loc["He2", "SCF/sto-3g"]

    # Check results
    assert pytest.approx(0.00024477933196125805, 1.e-5) == ds.statistics("MUE", "SCF/sto-3g")

    assert pytest.approx([0.081193, 7.9533e-05], 1.e-1) == list(ds.statistics("URE", "SCF/sto-3g"))
    assert pytest.approx(0.0406367, 1.e-5) == ds.statistics("MURE", "SCF/sto-3g")
    assert pytest.approx(0.002447793, 1.e-5) == ds.statistics("MURE", "SCF/sto-3g", floor=10)

    assert isinstance(ds.to_json(), dict)
    assert ds.list_records(keywords=None).shape[0] == 1

    ds.units = "eV"
    assert pytest.approx(0.00010614635, 1.e-5) == ds.statistics("MURE", "SCF/sto-3g", floor=10)

    # Check get_molecules
    mols = ds.get_molecules()
    assert mols.shape == (2, 1)

    mols = ds.get_molecules(stoich="cp1")
    assert mols.shape == (2, 1)


@testing.using_psi4
def test_compute_reactiondataset_keywords(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 -1.1\n--\nHe 0 0 1.1")

    # Build a dataset
    ds = ptl.collections.ReactionDataset("dataset_options", client, ds_type="ie")
    ds.set_default_program("Psi4")

    ds.add_ie_rxn("He2", mol1)
    ds.add_keywords("direct", "psi4", ptl.models.KeywordSet(values={"scf_type": "direct"}), default=True)
    ds.add_keywords("df", "psi4", ptl.models.KeywordSet(values={"scf_type": "df"}))

    ds.save()
    ds = client.get_collection("reactiondataset", "dataset_options")

    # Compute, should default to direct options
    r = ds.compute("SCF", "STO-3G")
    fractal_compute_server.await_results()
    ret = ds.get_values("SCF", "STO-3G")
    assert pytest.approx(0.39323818102293856, 1.e-5) == ds.df.loc["He2", "SCF/sto-3g"]

    r = ds.compute("SCF", "sto-3g", keywords="df")
    fractal_compute_server.await_results()
    ds.get_values("SCF", "sto-3g", keywords="df").columns[0] == "SCF/sto-3g-df"
    assert pytest.approx(0.38748602675524185, 1.e-5) == ds.df.loc["He2", "SCF/sto-3g-df"]

    assert ds.list_records().shape[0] == 2
    assert ds.list_records(keywords="DF").shape[0] == 1
    assert ds.list_records(keywords="DIRECT").shape[0] == 1

    # Check saved history
    ds = client.get_collection("reactiondataset", "dataset_options")
    assert ds.list_records().shape[0] == 2
    assert {"df", "direct"} == set(ds.list_records().reset_index()["keywords"])

    # Check keywords
    kw = ds.get_keywords("df", "psi4")
    assert kw.values["scf_type"] == "df"


def test_dataset_list_get_values(gradient_dataset_fixture, contributed_dataset_fixture,
                                 reactiondataset_dftd3_fixture_fixture):
    """ Tests that the output of list_values can be used as input to get_values"""
    for dataset_fixture in locals().values():
        client, ds = dataset_fixture

        columns = ds.list_values().reset_index()

        for row in columns.to_dict("records"):
            spec = row.copy()
            name = spec.pop("name")
            if "stoichiometry" in spec:
                spec["stoich"] = spec.pop("stoichiometry")
            from_name = ds.get_values(name=name)
            from_spec = ds.get_values(**spec)
            assert from_name.shape == (len(ds.get_index()), 1)
            assert from_spec.shape == (len(ds.get_index()), 1)
            assert from_name.columns[0] == from_spec.columns[0]


def test_generic_collection(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)
    g = ptl.collections.Generic("Generic1", client=client)

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
    g2 = ptl.collections.Generic.from_server(client, "Generic1")
    assert g2["hello"] == 5


def test_missing_collection(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)
    with pytest.raises(KeyError):
        client.get_collection("reactiondataset", "_waffles_")


@pytest.mark.slow
@testing.using_torsiondrive
@testing.using_geometric
@testing.using_rdkit
def test_torsiondrive_dataset(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    ds = ptl.collections.TorsionDriveDataset("testing", client=client)

    hooh1 = ptl.data.get_molecule("hooh.json")
    hooh2 = hooh1.copy(update={"geometry": hooh1.geometry + np.array([0, 0, 0.2])})

    ds.add_entry("hooh1", [hooh1], [[0, 1, 2, 3]], [90], attributes={"something": "hooh1"})
    ds.add_entry("hooh2", [hooh2], [[0, 1, 2, 3]], [90], attributes={"something": "hooh2"})

    optimization_spec = {
        "program": "geometric",
        "keywords": {
            "coordsys": "tric",
        }
    }
    qc_spec = {
        "driver": "gradient",
        "method": "UFF",
        "basis": "",
        "keywords": None,
        "program": "rdkit",
    }

    ds.add_specification("Spec1", optimization_spec, qc_spec, description="This is a really cool spec")

    ncompute = ds.compute("spec1")
    assert ncompute == 2
    assert ds.status("spec1")["Spec1"].sum() == 2  # Might have completed from previous run.

    ds.save()

    fractal_compute_server.await_services(max_iter=1)

    # Check status
    status_detail = ds.status("Spec1", detail=True)
    assert status_detail.loc["hooh2", "Complete"] == 1
    assert status_detail.loc["hooh2", "Total Points"] == 4

    fractal_compute_server.await_services(max_iter=5)

    ds = client.get_collection("torsiondrivedataset", "testing")
    ds.query("spec1")

    # Add another fake set, should instantly return
    ds.add_specification("spec2", optimization_spec, qc_spec, description="This is a really cool spec")

    # Test subsets
    ncompute = ds.compute("spec2", subset=set())
    assert ncompute == 0

    ncompute = ds.compute("spec2")
    assert ncompute == 2
    ds.query("spec2")

    # We effectively computed the same thing twice with two duplicate specs
    for row in ["hooh1", "hooh2"]:
        for spec in ["Spec1", "spec2"]:
            assert pytest.approx(ds.df.loc[row, spec].get_final_energies(90), 1.e-5) == 0.00015655375994799847

    assert ds.status().loc["COMPLETE", "Spec1"] == 2
    assert ds.status(collapse=False).loc["hooh1", "Spec1"] == "COMPLETE"

    assert ds.counts("hooh1").loc["hooh1", "Spec1"] > 5
    assert ds.counts("hooh1", specs="spec1", count_gradients=True).loc["hooh1", "Spec1"] > 30


@testing.using_geometric
@testing.using_rdkit
def test_optimization_dataset(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    ds = ptl.collections.OptimizationDataset("testing", client=client)

    opt_spec = {"program": "geometric"}
    qc_spec = {"driver": "gradient", "method": "UFF", "program": "rdkit"}
    ds.add_specification("test", opt_spec, qc_spec)

    hooh1 = ptl.data.get_molecule("hooh.json")
    hooh2 = hooh1.copy(update={"geometry": hooh1.geometry + np.array([0, 0, 0.2])})

    ds.add_entry("hooh1", hooh1)
    ds.add_entry("hooh1-2", hooh1)
    ds.add_entry("hooh2", hooh2)

    ds.compute("test")
    fractal_compute_server.await_results()

    ds.query("test")
    assert ds.status().loc["COMPLETE", "test"] == 3

    assert ds.counts().loc["hooh1", "test"] >= 4

    final_energy = 0.00011456853977485626
    for idx, row in ds.df["test"].items():
        assert pytest.approx(row.get_final_energy(), abs=1.e-5) == final_energy

    opt = ds.get_record("hooh1", "test")
    assert pytest.approx(opt.get_final_energy(), abs=1.e-5) == final_energy


@testing.using_geometric
@testing.using_rdkit
def test_grid_optimization_dataset(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    ds = ptl.collections.GridOptimizationDataset("testing", client=client)

    opt_spec = {"program": "geometric", "keywords": {}}
    qc_spec = {"driver": "gradient", "method": "UFF", "program": "rdkit"}
    ds.add_specification("test", opt_spec, qc_spec)

    hooh1 = ptl.data.get_molecule("hooh.json")

    scans = [{"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-10, 10], "step_type": "relative"}]
    ds.add_entry("hooh1", hooh1, scans=scans, preoptimization=False)

    ds.compute("test")
    fractal_compute_server.await_services()

    ds.query("test")
    assert ds.get_record("hooh1", "test").status == "COMPLETE"
