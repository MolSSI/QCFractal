"""
Tests the server collection compute capabilities.
"""
import itertools
import pathlib
from contextlib import contextmanager
from typing import List

import numpy as np
import pandas as pd
import pytest
import qcelemental as qcel
from qcelemental.models import Molecule, ProtoModel

import qcfractal.interface as ptl
from qcengine.testing import is_program_new_enough
from qcfractal import testing
from qcfractal.testing import df_compare, fractal_compute_server, live_fractal_or_skip


@contextmanager
def check_requests_monitor(client, request, request_made=True, kind="get"):
    before = client._request_counter[(request, kind)]
    yield
    after = client._request_counter[(request, kind)]

    if request_made:
        assert after > before, f"Requests were were expected, but none were made. Request type: {request} ({kind})"
    else:
        assert after == before, f"Requests were made when none were expected. Request type: {request} ({kind})"


def handle_dataset_fixture_params(client, ds_type, ds, fractal_compute_server, request):
    ds = client.get_collection(ds_type, ds.name)
    if request.param == "no_view":
        ds._disable_view = True
    elif request.param == "download_view":
        ds._disable_view = False
        try:
            import requests_mock
        except ImportError:
            pytest.skip("Missing request_mock")
        with requests_mock.Mocker(real_http=True) as m:
            with open(fractal_compute_server.view_handler.view_path(ds.data.id), "rb") as f:
                m.get(ds.data.view_url_hdf5, body=f)
                ds.download(verify=True)
    elif request.param == "remote_view":
        ds._disable_view = False
        if ds._view is None:
            raise ValueError("Remote view is not available.")
        assert isinstance(ds._view, ptl.collections.RemoteView)
    else:
        raise ValueError(f"Unknown dataset fixture parameter: {request.param}.")
    return ds


def build_dataset_fixture_view(ds, fractal_compute_server):
    view = ptl.collections.HDF5View(fractal_compute_server.view_handler.view_path(ds.data.id))
    view.write(ds)
    ds.data.__dict__["view_available"] = True
    ds.data.__dict__["view_url_hdf5"] = f"http://mock.repo/{ds.data.id}/latest.hdf5"
    ds.data.__dict__["view_metadata"] = {"blake2b_checksum": view.hash()}
    ds.save()


@pytest.fixture(scope="module", params=["download_view", "no_view", "remote_view"])
def gradient_dataset_fixture(fractal_compute_server, tmp_path_factory, request):
    client = ptl.FractalClient(fractal_compute_server)

    try:
        ds = client.get_collection("Dataset", "ds_gradient")
    except KeyError:
        testing.check_has_module("psi4")

        # Build a dataset
        ds = ptl.collections.Dataset(
            "ds_gradient", client, default_program="psi4", default_driver="gradient", default_units="hartree/bohr"
        )

        ds.add_entry("He1", ptl.Molecule.from_data("He -1 0 0\n--\nHe 0 0 1"))
        ds.add_entry("He2", ptl.Molecule.from_data("He -1.1 0 0\n--\nHe 0 0 1.1"))
        ds.save()

        contrib = {
            "name": "Gradient",
            "theory_level": "pseudo-random values",
            "values": [[0.03, 0, 0.02, -0.02, 0, -0.03], [0.03, 0, 0.02, -0.02, 0, -0.03]],
            "index": ["He1", "He2"],
            "theory_level_details": {"driver": "gradient"},
            "units": "hartree/bohr",
        }
        contrib_no_details = {
            "name": "no details",
            "theory_level": "pseudo-random values",
            "values": [[0.03, 0, 0.02, -0.02, 0, -0.03], [0.03, 0, 0.02, -0.02, 0, -0.03]],
            "index": ["He1", "He2"],
            "units": "hartree/bohr",
        }
        contrib_all_details = {
            "name": "all details",
            "theory_level": "pseudo-random values",
            "values": [[0.03, 0, 0.02, -0.02, 0, -0.03], [0.03, 0, 0.02, -0.02, 0, -0.03]],
            "index": ["He1", "He2"],
            "theory_level_details": {
                "driver": "gradient",
                "program": "fake_program",
                "basis": "fake_basis",
                "method": "fake_method",
                "keywords": "fake_keywords",
            },
            "units": "hartree/bohr",
        }
        ds.add_contributed_values(contrib)
        ds.add_contributed_values(contrib_all_details)
        ds.add_contributed_values(contrib_no_details)

        ds.add_keywords("scf_default", "psi4", ptl.models.KeywordSet(values={}), default=True)
        ds.save()

        ds.compute("HF", "sto-3g")
        ds.compute("HF", "3-21g")
        fractal_compute_server.await_results()

        assert ds.get_records("HF", "sto-3g").iloc[0, 0].status == "COMPLETE"
        assert ds.get_records("HF", "sto-3g").iloc[1, 0].status == "COMPLETE"
        assert ds.get_records("HF", "3-21g").iloc[0, 0].status == "COMPLETE"
        assert ds.get_records("HF", "3-21g").iloc[1, 0].status == "COMPLETE"

        build_dataset_fixture_view(ds, fractal_compute_server)

    ds = handle_dataset_fixture_params(client, "Dataset", ds, fractal_compute_server, request)

    yield client, ds


def test_gradient_dataset_get_molecules(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture
    request_made = not ds._use_view(False)
    ds._clear_cache()

    he1_dist = 2.672476322216822
    he2_dist = 2.939723950195864
    with check_requests_monitor(client, "molecule", request_made=request_made):
        mols = ds.get_molecules()
    assert mols.shape == (2, 1)
    assert mols.iloc[0, 0].measure([0, 1]) == pytest.approx(he1_dist)
    assert mols.iloc[1, 0].measure([0, 1]) == pytest.approx(he2_dist)

    mol = ds.get_molecules(subset="He1")
    assert mol.measure([0, 1]) == pytest.approx(he1_dist)

    mol = ds.get_molecules(subset="He2")
    assert mol.measure([0, 1]) == pytest.approx(he2_dist)

    mols_subset = ds.get_molecules(subset=["He1"])
    assert mols_subset.iloc[0, 0].measure([0, 1]) == pytest.approx(he1_dist)

    with pytest.raises((KeyError, RuntimeError)):
        ds.get_molecules(subset="NotInDataset")


def test_gradient_dataset_get_records(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    with check_requests_monitor(client, "result", request_made=True):
        records = ds.get_records("HF", "sto-3g")

    assert records.shape == (2, 1)
    assert records.iloc[0, 0].status == "COMPLETE"
    assert records.iloc[1, 0].status == "COMPLETE"

    with check_requests_monitor(client, "result", request_made=True):
        records_subset1 = ds.get_records("HF", "sto-3g", subset="He2")
    assert records_subset1.status == "COMPLETE"

    with check_requests_monitor(client, "result", request_made=True):
        records_subset2 = ds.get_records("HF", "sto-3g", subset=["He2"])
    assert records_subset2.shape == (1, 1)
    assert records_subset2.iloc[0, 0].status == "COMPLETE"

    with check_requests_monitor(client, "result", request_made=True):
        rec_proj = ds.get_records("HF", "sto-3g", include=["extras", "return_result"])
    assert rec_proj.shape == (2, 2)
    assert set(rec_proj.columns) == {"extras", "return_result"}

    with pytest.raises(KeyError):
        with check_requests_monitor(client, "result", request_made=False):
            ds.get_records(method="NotInDataset")


def test_gradient_dataset_get_values(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    request_made = not ds._use_view(False)
    ds._clear_cache()

    with check_requests_monitor(client, "result", request_made=request_made):
        cols = set(ds.get_values().columns)
    names = set(ds.list_values().reset_index()["name"])
    assert cols == names

    df = ds.get_values()
    assert df.shape == (len(ds.get_index()), 5)
    for entry in ds.get_index():
        assert (
            df.loc[entry, "HF/sto-3g"]
            == ds.get_records(subset=entry, method="hf", basis="sto-3g", include=["return_result"])
        ).all()

    assert ds.get_values(method="NotInDataset").shape[1] == 0  # 0-length DFs can cause exceptions

    with pytest.warns(RuntimeWarning):
        ds.get_values(name="HF/sto-3g", basis="sto-3g")


def test_gradient_dataset_list_values(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    # List contributed values
    df = ds.list_values(native=False).reset_index()

    assert df.shape == (3, 7)
    assert set(df.columns) == {*ds.data.history_keys, "name", "native"}
    assert {x.lower() for x in df["name"]} == set(ds.data.contributed_values.keys())

    df = ds.list_values(driver="graDieNt", native=False).reset_index()
    assert {x.lower() for x in df["name"]} == {"gradient", "all details"}

    names = ["GrAdIeNt", "no details"]
    df = ds.list_values(name=names, native=False).reset_index()
    assert {x.lower() for x in df["name"]} == {name.lower() for name in names}

    # List native values
    df1 = ds.list_values(native=True).reset_index()
    assert df1.shape == (2, 7)
    assert set(df1.columns) == {*ds.data.history_keys, "name", "native"}

    df2 = ds.list_values(method="hf", basis="sto-3g", native=True).reset_index()
    df3 = ds.list_values(name="hf/sto-3g", native=True).reset_index()
    assert (df2 == df3).all().all()

    # All values
    df = ds.list_values().reset_index()
    assert df.shape == (5, 7)
    assert set(df.columns) == {*ds.data.history_keys, "name", "native"}

    df = ds.list_values(driver="gradient").reset_index()
    assert df.shape == (4, 7)

    df = ds.list_values(name="Not in dataset").reset_index()
    assert len(df) == 0


def test_gradient_dataset_statistics(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    df = ds.get_values(native=True)
    assert df.shape == (2, 2)
    assert np.sum(df.loc["He2", "HF/sto-3g"]) == pytest.approx(0.0)

    # Test out some statistics
    stats = ds.statistics("MUE", "HF/sto-3g", "Gradient")
    assert pytest.approx(stats.mean(), 1.0e-5) == 0.00984176986312362

    stats = ds.statistics("UE", "HF/sto-3g", "Gradient")
    assert pytest.approx(stats.loc["He1"].mean(), 1.0e-5) == 0.01635020639
    assert pytest.approx(stats.loc["He2"].mean(), 1.0e-5) == 0.00333333333


def test_gradient_dataset_get_values_caching(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    ds._clear_cache()

    with check_requests_monitor(client, "result", request_made=True and not ds._use_view(False)):
        ds.get_values()

    with check_requests_monitor(client, "result", request_made=False):
        ds.get_values()

    ds._clear_cache()

    with check_requests_monitor(client, "result", request_made=True and not ds._use_view(False)):
        ds.get_values(basis="sto-3g")

    with check_requests_monitor(client, "result", request_made=False):
        ds.get_values(basis="sto-3g", subset=["He1"])
        ds.get_values(basis="sto-3g", subset="He2")
        ds.get_values(basis="sto-3g", subset=["He1", "He2"])

    with check_requests_monitor(client, "result", request_made=True and not ds._use_view(False)):
        ds.get_values(basis="3-21g", subset="He1")

    with check_requests_monitor(client, "result", request_made=True and not ds._use_view(False)):
        ds.get_values(basis="3-21g", subset=["He1", "He2"])

    with check_requests_monitor(client, "result", request_made=False):
        ds.get_values(basis="3-21g", subset="He2")

    with check_requests_monitor(client, "result", request_made=False):
        ds.get_values()


@pytest.mark.parametrize("use_cache", [True, False])
def test_gradient_dataset_values_subset(gradient_dataset_fixture, use_cache):
    client, ds = gradient_dataset_fixture

    allvals = ds.get_values()
    ds._clear_cache()

    subsets = [None, "He1", ["He2"], ["He1", "He2"], ["He1", "He2", "He1", "He1"]]
    colnames = [None, "HF/3-21g", ["HF/3-21g", "HF/sto-3g"], "no details", ["HF/sto-3g", "no details"]]
    for subset, colname in itertools.product(subsets, colnames):
        if not use_cache:
            ds._clear_cache()
        df1 = ds.get_values(subset=subset, name=colname)

        if colname is None:
            c = slice(None)
        elif isinstance(colname, str):
            c = [colname]
        else:
            c = colname
        df2 = allvals.loc[subset if subset is not None else slice(None), c]
        assert df_compare(df1, df2, sort=True)


def test_gradient_dataset_records_args(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    assert len(ds.get_records(method="hf", keywords="scf_default", basis="sto-3g")) == 2
    assert (
        len(ds.get_records(method="hf", keywords="scf_default")) == 4
    )  # TODO: we might want a multi-keyword dataset fixture here
    assert len(ds.get_records(method="hf")) == 4


@pytest.fixture(scope="module", params=["download_view", "no_view", "remote_view"])
def contributed_dataset_fixture(fractal_compute_server, tmp_path_factory, request):
    """ Fixture for testing rich contributed datasets with many properties and molecules of different sizes"""
    client = ptl.FractalClient(fractal_compute_server)
    try:
        ds = client.get_collection("Dataset", "ds_contributed")
    except KeyError:

        # Build a dataset
        ds = ptl.collections.Dataset("ds_contributed", client)

        ds.add_entry("He1", ptl.Molecule.from_data("He -1 0 0\n--\nHe 0 0 1"))
        ds.add_entry("He", ptl.Molecule.from_data("He -1.1 0 0"))
        ds.units = "hartree"
        ds.save()

        energy = {
            "name": "Fake Energy",
            "theory_level": "pseudo-random values",
            "values": np.array([1234.5, 5.4321]),
            "index": ["He1", "He"],
            "theory_level_details": {
                "driver": "energy",
                "program": "fake_program",
                "basis": "fake_basis",
                "method": "fake_method",
            },
            "units": "hartree",
        }
        gradient = {
            "name": "Fake Gradient",
            "theory_level": "pseudo-random values",
            "values": [np.array([0.03, 0, 0.02, -0.02, 0, -0.03]), np.array([0.03, 0, 0.02])],
            "index": ["He1", "He"],
            "theory_level_details": {
                "driver": "gradient",
                "program": "fake_program",
                "basis": "fake_basis",
                "method": "fake_method",
            },
            "units": "hartree/bohr",
        }
        hessian = {
            "name": "Fake Hessian",
            "theory_level": "pseudo-random values",
            "values": [np.eye(6).ravel(), np.array([1, 0.2, 0.1, 0.2, 1, 0.4, 0.1, 0.4, 1])],
            "index": ["He1", "He"],
            "theory_level_details": {
                "driver": "hessian",
                "program": "fake_program",
                "basis": "fake_basis",
                "method": "fake_method",
            },
            "units": "hartree/bohr**2",
        }
        dipole = {
            "name": "Fake Dipole",
            "theory_level": "pseudo-random values",
            "values": [np.array([1.0, 2.0, 3.0]), np.array([1.0, -2.0, 0.0])],
            "index": ["He1", "He"],
            "theory_level_details": {
                "driver": "dipole",
                "program": "fake_program",
                "basis": "fake_basis",
                "method": "fake_method",
            },
            "units": "e * bohr",
        }
        energy_FF = {
            "name": "Fake FF Energy",
            "theory_level": "some force field",
            "values": [0.5, 42.0],
            "index": ["He1", "He"],
            "theory_level_details": {
                "driver": "energy",
                "program": "fake_program",
                "basis": None,
                "method": "fake_method",
            },
            "units": "hartree",
        }

        ds.add_contributed_values(energy)
        ds.add_contributed_values(gradient)
        ds.add_contributed_values(hessian)
        ds.add_contributed_values(dipole)
        ds.add_contributed_values(energy_FF)

        with pytest.raises(KeyError):
            ds.add_contributed_values(energy)

        ds.save()

        build_dataset_fixture_view(ds, fractal_compute_server)

    ds = handle_dataset_fixture_params(client, "Dataset", ds, fractal_compute_server, request)

    yield client, ds


def test_dataset_contributed_units(contributed_dataset_fixture):
    _, ds = contributed_dataset_fixture

    assert qcel.constants.ureg(
        ds._column_metadata[ds.get_values(name="Fake Energy").columns[0]]["units"]
    ) == qcel.constants.ureg(ds.units)
    assert qcel.constants.ureg(
        ds._column_metadata[ds.get_values(name="Fake Gradient").columns[0]]["units"]
    ) == qcel.constants.ureg("hartree/bohr")
    assert qcel.constants.ureg(
        ds._column_metadata[ds.get_values(name="Fake Hessian").columns[0]]["units"]
    ) == qcel.constants.ureg("hartree/bohr**2")
    assert qcel.constants.ureg(
        ds._column_metadata[ds.get_values(name="Fake Dipole").columns[0]]["units"]
    ) == qcel.constants.ureg("e * bohr")

    old_units = ds.units
    assert old_units == "kcal / mol"
    ds.units = "kcal/mol"
    before = ds.get_values()
    ds.units = "hartree"
    after = ds.get_values()
    assert before["Fake Energy"][0] == after["Fake Energy"][0] / qcel.constants.conversion_factor("kcal/mol", "hartree")
    assert before["Fake Gradient"][0][0, 0] == after["Fake Gradient"][0][0, 0]
    ds.units = old_units


def test_dataset_contributed_mixed_values(contributed_dataset_fixture):
    _, ds = contributed_dataset_fixture

    unselected_values = ds.get_values()
    assert unselected_values.shape == (2, 5)
    selected_values = ds.get_values(program="fake_program")
    assert selected_values.shape == (2, 5)
    selected_values = ds.get_values(basis="None")
    assert selected_values.shape == (2, 1)
    assert selected_values.columns[0] == "Fake FF Energy"


def test_dataset_compute_response(fractal_compute_server):
    """ Tests that the full compute response is returned when calling Dataset.compute """
    client = ptl.FractalClient(fractal_compute_server)

    # Build a dataset
    ds = ptl.collections.Dataset("ds", client, default_program="psi4", default_driver="energy", default_units="hartree")

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


@testing.using_psi4
def test_dataset_protocols(fractal_compute_server):
    """ Tests using protocols with dataset compute."""
    client = ptl.FractalClient(fractal_compute_server)

    # Build basis dataset
    ds = ptl.collections.Dataset("protocol_dataset", client, default_program="psi4", default_driver="energy")

    ds.add_entry("He1", ptl.Molecule.from_data("He 0 0 0\n--\nHe 0 0 2.2"))
    ds.save()

    # compute the wavefunction
    response = ds.compute(method="hf", basis="sto-3g", protocols={"wavefunction": "orbitals_and_eigenvalues"})

    # await the result and check for orbitals
    fractal_compute_server.await_results()

    result = client.query_results(id=response.ids)[0]
    orbitals = result.get_wavefunction("orbitals_a")
    assert orbitals.shape == (2, 2)

    basis = result.get_wavefunction("basis")
    assert basis.name.lower() == "sto-3g"


def test_reactiondataset_check_state(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)
    ds = ptl.collections.ReactionDataset("check_state", client, ds_type="ie", default_program="rdkit")
    ds.add_ie_rxn("He1", ptl.Molecule.from_data("He -3 0 0\n--\nNe 0 0 2"))

    with pytest.raises(ValueError):
        ds.compute("SCF", "STO-3G")

    with pytest.raises(ValueError):
        ds.get_records("SCF", "STO-3G")

    ds.save()

    ds.add_keywords("default", "psi4", ptl.models.KeywordSet(values={"a": 5}))

    with pytest.raises(ValueError):
        ds.get_records("SCF", "STO-3G")

    ds.save()

    contrib = {
        "name": "Benchmark",
        "doi": None,
        "theory_level": "very high",
        "values": [-0.00001098794749, 0.0009608501557],
        "index": ["He2", "He1"],
        "units": "hartree",
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
    assert bench.loc["He1"][0] == contrib["values"][1]
    assert bench.loc["He2"][0] == contrib["values"][0]


@pytest.mark.parametrize("use_cache", [True, False])
def test_contributed_dataset_values_subset(contributed_dataset_fixture, use_cache):
    client, ds = contributed_dataset_fixture

    ds._clear_cache()
    allvals = ds.get_values()
    ds._clear_cache()

    subsets = [None, "He1", ["He"], ["He", "He1"]]
    colnames = [None, "Fake Energy", ["Fake Hessian", "Fake Dipole", "Fake Energy"]]
    for subset, colname in itertools.product(subsets, colnames):
        if not use_cache:
            ds._clear_cache()
        df1 = ds.get_values(subset=subset, name=colname)

        if colname is None:
            c = slice(None)
        elif isinstance(colname, str):
            c = [colname]
        else:
            c = colname
        df2 = allvals.loc[subset if subset is not None else slice(None), c]
        assert df_compare(df1, df2, sort=True)


@pytest.fixture(scope="module", params=["no_view", "remote_view", "download_view"])
def reactiondataset_dftd3_fixture_fixture(fractal_compute_server, tmp_path_factory, request):
    ds_name = "He_DFTD3"
    client = ptl.FractalClient(fractal_compute_server)

    try:
        ds = client.get_collection("ReactionDataset", ds_name)
    except KeyError:
        testing.check_has_module("psi4")
        testing.check_has_module("dftd3")

        ds = ptl.collections.ReactionDataset(ds_name, client, ds_type="ie", default_units="hartree")

        # Add two helium dimers to the DB at 4 and 8 bohr
        HeDimer = ptl.Molecule.from_data([[2, 0, 0, -1.412], [2, 0, 0, 1.412]], dtype="numpy", units="bohr", frags=[1])
        ds.add_ie_rxn("HeDimer", HeDimer, attributes={"r": 4})
        ds.set_default_program("psi4")
        ds.add_keywords("scf_default", "psi4", ptl.models.KeywordSet(values={"e_convergence": 1.0e-10}), default=True)

        ds.save()

        ncomp1 = ds.compute("B3LYP-D3", "6-31g")
        assert len(ncomp1.ids) == 4
        assert len(ncomp1.submitted) == 4  # Dimer/monomer, dft/dftd3

        ncomp1 = ds.compute("B3LYP-D3", "6-31g", stoich="cp")
        assert len(ncomp1.ids) == 4
        assert len(ncomp1.submitted) == 2  # monomer, dft/dftd3

        ncomp2 = ds.compute("B3LYP-D3(BJ)", "6-31g")
        assert len(ncomp2.ids) == 4
        assert len(ncomp2.submitted) == 2  # dimer/monomer, dftd3

        ncomp1 = ds.compute("B3LYP-D3(BJ)", "6-31g", stoich="cp")
        assert len(ncomp1.ids) == 4
        assert len(ncomp1.submitted) == 1  # monomer, dftd3

        fractal_compute_server.await_results()

        build_dataset_fixture_view(ds, fractal_compute_server)

    ds = handle_dataset_fixture_params(client, "ReactionDataset", ds, fractal_compute_server, request)

    yield client, ds


def test_reactiondataset_dftd3_records(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture

    records = ds.get_records("B3LYP", "6-31g")
    assert records.shape == (1, 1)
    assert records.iloc[0, 0].status == "COMPLETE"

    records = ds.get_records("B3LYP", "6-31g", stoich="default")
    assert records.shape == (1, 1)
    assert records.iloc[0, 0].status == "COMPLETE"

    records = ds.get_records("B3LYP", "6-31g", stoich="default", subset="HeDimer")
    assert records.shape == (1, 1)

    # No molecules
    with pytest.raises(KeyError):
        records = ds.get_records("B3LYP", "6-31g", stoich="default", subset="Gibberish")

    # Bad stoichiometry
    with pytest.raises(KeyError):
        ds.get_records("B3LYP", "6-31g", stoich="cp5")


def test_reactiondataset_dftd3_energies(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture

    request_made = not ds._use_view(False)
    ds._clear_cache()

    bench = {
        "B3LYP/6-31g": pytest.approx(0.01822563, abs=1.0e-5),
        "cp-B3LYP/6-31g": pytest.approx(0.01867427, abs=1.0e-5),
        "B3LYP-D3/6-31g": pytest.approx(0.01815022, abs=1.0e-5),
        "cp-B3LYP-D3/6-31g": pytest.approx(0.01859886, abs=1.0e-5),
        "B3LYP-D3(BJ)/6-31g": pytest.approx(0.01814335, abs=1.0e-5),
        "cp-B3LYP-D3(BJ)/6-31g": pytest.approx(0.01859199, abs=1.0e-5),
    }

    with check_requests_monitor(client, "result", request_made=request_made):
        ret = ds.get_values("B3LYP", "6-31G")
    assert ret.loc["HeDimer", "B3LYP/6-31g"] == bench["B3LYP/6-31g"]

    ret = ds.get_values("B3LYP", "6-31G", stoich="cp")
    assert ret.loc["HeDimer", "cp-B3LYP/6-31g"] == bench["cp-B3LYP/6-31g"]

    ret = ds.get_values("B3LYP-D3", "6-31G")
    assert ret.loc["HeDimer", "B3LYP-D3/6-31g"] == bench["B3LYP-D3/6-31g"]

    ret = ds.get_values("B3LYP-D3(BJ)", "6-31G")
    assert ret.loc["HeDimer", "B3LYP-D3(BJ)/6-31g"] == bench["B3LYP-D3(BJ)/6-31g"]

    # Should be in ds.df now as well
    ds.get_values(stoich="cp")
    for key, value in bench.items():
        assert value == ds.df.loc["HeDimer", key]


def test_reactiondataset_dftd3_molecules(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture

    request_made = not ds._use_view(False)
    ds._clear_cache()

    with check_requests_monitor(client, "molecule", request_made=request_made):
        mols = ds.get_molecules()
    assert mols.shape == (1, 1)
    assert np.all(mols.iloc[0, 0].real)  # Should be all real
    assert tuple(mols.index) == (("HeDimer", "default", 0),)

    mols = ds.get_molecules(stoich="cp1")
    assert mols.shape == (1, 1)
    assert not np.all(mols.iloc[0, 0].real)  # Should be half real
    assert tuple(mols.index) == (("HeDimer", "cp1", 0),)

    stoichs = ["cp1", "default1", "cp", "default"]
    mols = ds.get_molecules(stoich=stoichs)
    assert mols.shape == (4, 1)

    mols = mols.reset_index()
    assert set(stoichs) == set(mols["stoichiometry"])


def test_rectiondataset_dftd3_values_caching(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture
    ds._clear_cache()

    with check_requests_monitor(client, "result", request_made=True and not ds._use_view(False)):
        ds.get_values("B3LYP", "6-31G")

    with check_requests_monitor(client, "result", request_made=True and not ds._use_view(False)):
        ds.get_values("B3LYP-D3", "6-31G")

    with check_requests_monitor(client, "result", request_made=True and not ds._use_view(False)):
        ds.get_values("B3LYP-D3(BJ)", "6-31G")

    with check_requests_monitor(client, "result", request_made=False):
        ds.get_values("B3LYP", "6-31G", subset=None)
        ds.get_values("B3LYP", "6-31G", subset="HeDimer")
        ds.get_values("B3LYP", "6-31G", subset=["HeDimer"])


@pytest.mark.parametrize("use_cache", [True, False])
def test_reactiondataset_dftd3_values_subset(reactiondataset_dftd3_fixture_fixture, use_cache):
    client, ds = reactiondataset_dftd3_fixture_fixture

    allvals = ds.get_values()
    ds._clear_cache()

    subsets = [None, "HeDimer", ["HeDimer"], ["HeDimer", "HeDimer"]]
    colnames = [None, "B3LYP/6-31g", ["B3LYP-D3/6-31g", "B3LYP-D3(BJ)/6-31g"], ["B3LYP/6-31g"]]
    for subset, colname in itertools.product(subsets, colnames):
        if not use_cache:
            ds._clear_cache()
        df1 = ds.get_values(subset=subset, name=colname)

        if colname is None:
            c = slice(None)
        elif isinstance(colname, str):
            c = [colname]
        else:
            c = colname
        df2 = allvals.loc[subset if subset is not None else slice(None), c]
        assert df_compare(df1, df2, sort=True)


def test_dataset_dftd3(reactiondataset_dftd3_fixture_fixture):
    client, rxn_ds = reactiondataset_dftd3_fixture_fixture

    if not rxn_ds._use_view():
        ds_name = "He_DFTD3"
        ds = ptl.collections.Dataset(ds_name, client)

        HeDimer = rxn_ds.get_molecules(subset="HeDimer").iloc[0, 0]
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
        "values": [0.0009608501557, -0.00001098794749],
        "index": ["He1", "He2"],
        "units": "hartree",
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
    assert pytest.approx(0.6024530476, 1.0e-5) == ret.loc["He1", "SCF/sto-3g"]
    assert pytest.approx(-0.0068950359, 1.0e-5) == ret.loc["He2", "SCF/sto-3g"]

    # Check results
    assert pytest.approx(0.00024477933196125805, 1.0e-5) == ds.statistics("MUE", "SCF/sto-3g")

    assert pytest.approx([0.081193, 7.9533e-05], 1.0e-1) == list(ds.statistics("URE", "SCF/sto-3g"))
    assert pytest.approx(0.0406367, 1.0e-5) == ds.statistics("MURE", "SCF/sto-3g")
    assert pytest.approx(0.002447793, 1.0e-5) == ds.statistics("MURE", "SCF/sto-3g", floor=10)

    assert isinstance(ds.to_json(), dict)
    assert ds.list_records(keywords=None).shape[0] == 1

    ds.units = "eV"
    assert pytest.approx(0.00010614635, 1.0e-5) == ds.statistics("MURE", "SCF/sto-3g", floor=10)

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
    assert pytest.approx(0.39323818102293856, 1.0e-5) == ds.df.loc["He2", "SCF/sto-3g"]

    r = ds.compute("SCF", "sto-3g", keywords="df")
    fractal_compute_server.await_results()
    ds.get_values("SCF", "sto-3g", keywords="df").columns[0] == "SCF/sto-3g-df"
    assert pytest.approx(0.38748602675524185, 1.0e-5) == ds.df.loc["He2", "SCF/sto-3g-df"]

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


@pytest.fixture(scope="module", params=[True, False], ids=["local_view", "no_view"])
def qm3_fixture(request, tmp_path_factory):
    # Connect to the QCArchive
    client = live_fractal_or_skip()
    ds = client.get_collection("Dataset", "QM3")
    ds._disable_query_limit = True

    # Trim down dataset for faster test
    to_remove = {row for row in ds.data.history if row[2].lower() not in {"b3lyp", "pbe"}}
    for row in to_remove:
        ds.data.history.remove(row)
    ds.get_entries(force=True)

    # with view
    if request.param:
        view = ptl.collections.HDF5View(pathlib.Path(tmp_path_factory.mktemp("test_collections"), "ds_qm3.hdf5"))
        view.write(ds)
        ds._view = view
    else:
        ds._view = None

    yield client, ds


@pytest.fixture(scope="module", params=[True, False], ids=["local_view", "no_view"])
def s22_fixture(request, tmp_path_factory):
    # Connect to the QCArchive
    client = live_fractal_or_skip()
    ds = client.get_collection("ReactionDataset", "S22")
    ds._disable_query_limit = True

    # Trim down dataset for faster test
    to_remove = {row for row in ds.data.history if row[2].lower() not in {"b3lyp", "pbe"}}
    for row in to_remove:
        ds.data.history.remove(row)
    ds.get_entries(force=True)

    # with view
    if request.param:
        view = ptl.collections.HDF5View(pathlib.Path(tmp_path_factory.mktemp("test_collections"), "ds_s22.hdf5"))
        view.write(ds)
        ds._view = view
    else:
        ds._view = None

    yield client, ds


@pytest.mark.slow
def test_qm3_list_select(qm3_fixture):
    """ tests list_values and get_values with multiple selections on the method and basis field """
    client, ds = qm3_fixture

    methods = {"b3lyp", "pbe"}
    bases = {"def2-svp", "def2-tzvp"}
    names = {f"{method}/{basis}" for (method, basis) in itertools.product(methods, bases)}

    df = ds.list_values(method=["b3lyp", "pbe"], basis=["def2-svp", "def2-tzvp"]).reset_index()
    assert names == set(df["name"].str.lower())

    df = ds.get_values(method=["b3lyp", "pbe"], basis=["def2-svp", "def2-tzvp"])
    assert names == set(df.columns.str.lower())

    df = ds.list_values(name=list(names)).reset_index()
    assert names == set(df["name"].str.lower())

    df = ds.get_values(name=list(names))
    assert names == set(df.columns.str.lower())


@pytest.mark.slow
def test_s22_list_select(s22_fixture):
    """ tests list_values and get_values with multiple selections on the method and basis field """
    client, ds = s22_fixture

    methods = {"b3lyp", "pbe"}
    bases = {"def2-svp", "def2-tzvp"}
    names = {f"{method}/{basis}" for (method, basis) in itertools.product(methods, bases)}

    df = ds.list_values(method=["b3lyp", "pbe"], basis=["def2-svp", "def2-tzvp"]).reset_index()
    assert names == set(df[df["stoichiometry"] == "default"]["name"].str.lower())

    df = ds.get_values(method=["b3lyp", "pbe"], basis=["def2-svp", "def2-tzvp"], stoich="default")
    assert names == set(df.columns.str.lower())

    df = ds.get_values(name=list(names))
    assert names == set(df.columns.str.lower())


@testing.using_psi4
@testing.using_dftd3
def test_rds_rxn(fractal_compute_server):
    client = fractal_compute_server.client()
    ds = ptl.collections.ReactionDataset("rds_rxn", client, "rxn")
    HeDimer1 = ptl.Molecule.from_data([[2, 0, 0, -1.412], [2, 0, 0, 1.412]], dtype="numpy", units="bohr", frags=[1])
    HeDimer2 = ptl.Molecule.from_data([[2, 3, 3, -1.412], [2, 3, 3, 1.412]], dtype="numpy", units="bohr", frags=[1])

    ds.add_rxn("HeDimer1", stoichiometry={"default": [(HeDimer1, 0.0)]})
    ds.add_rxn("HeDimer2", stoichiometry={"default": [(HeDimer2, 1.0), (HeDimer1, -1.0)]})
    ds.set_default_program("psi4")
    ds.add_keywords("scf_default", "psi4", ptl.models.KeywordSet(values={"e_convergence": 1.0e-10}), default=True)

    ds.save()

    ds.compute("B3LYP-D3", "6-31g")
    ds.get_values(method="B3LYP-D3", basis="6-31g")


def assert_list_get_values(ds):
    """ Tests that the output of list_values can be used as input to get_values"""
    columns = ds.list_values().reset_index()
    all_specs_unique = len(columns.drop("name", axis=1).drop_duplicates()) == len(columns.drop("name", axis=1))
    for row in columns.to_dict("records"):
        spec = row.copy()
        name = spec.pop("name")
        if "stoichiometry" in spec:
            spec["stoich"] = spec.pop("stoichiometry")
        from_name = ds.get_values(name=name)
        from_spec = ds.get_values(**spec)
        assert from_name.shape == (len(ds.get_index()), 1)
        if not all_specs_unique:
            continue
        assert from_spec.shape == (len(ds.get_index()), 1)
        assert from_name.columns[0] == from_spec.columns[0]


def test_gradient_dataset_list_get_values(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture
    assert_list_get_values(ds)


def test_contributed_dataset_list_get_values(contributed_dataset_fixture):
    client, ds = contributed_dataset_fixture
    assert_list_get_values(ds)


def test_d3_dataset_list_get_values(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture
    assert_list_get_values(ds)


@pytest.mark.slow
def test_qm3_list_get_values(qm3_fixture):
    client, ds = qm3_fixture
    assert_list_get_values(ds)


@pytest.mark.slow
def test_s22_list_get_values(s22_fixture):
    client, ds = s22_fixture
    assert_list_get_values(ds)


def assert_view_identical(ds):
    """ Tests if get_values, list_values, get_entries, and get_molecules return the same result with/out a view"""

    ds._disable_view = True
    list_ds = ds.list_values(force=True)
    cv_ds = ds.get_values(native=False, force=True)
    nv_ds = ds.get_values(native=True, force=True)
    v_ds = ds.get_values(force=True)
    entry_ds = ds.get_entries(force=True)
    mol_ds = ds.get_molecules(force=True)

    ds._disable_view = False
    list_view = ds.list_values()
    cv_view = ds.get_values(native=False)
    nv_view = ds.get_values(native=True)
    v_view = ds.get_values()
    entry_view = ds.get_entries()
    mol_view = ds.get_molecules()

    # 'no details' column in gradient_dataset_fixture does not match
    # because there is not enough info to interpret it as a gradient
    if "no details" in cv_view.columns:
        cv_view.drop("no details", axis=1, inplace=True)
        cv_ds.drop("no details", axis=1, inplace=True)
        v_view.drop("no details", axis=1, inplace=True)
        v_ds.drop("no details", axis=1, inplace=True)

    # Molecule IDs are different on the server and in the view by design
    for mid in ["molecule", "molecule_id"]:
        if mid in entry_ds.columns:
            entry_ds.drop(mid, axis=1, inplace=True)
            entry_view.drop(mid, axis=1, inplace=True)

    assert list_ds.equals(list_view)
    assert df_compare(cv_view, cv_ds)
    assert df_compare(nv_view, nv_ds)
    assert df_compare(v_view, v_ds)
    assert df_compare(entry_ds, entry_view)
    assert df_compare(mol_ds, mol_view)


def test_gradient_dataset_view_identical(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture
    assert_view_identical(ds)


def test_contributed_dataset_view_identical(contributed_dataset_fixture):
    client, ds = contributed_dataset_fixture
    assert_view_identical(ds)


def test_d3_dataset_view_identical(reactiondataset_dftd3_fixture_fixture):
    client, ds = reactiondataset_dftd3_fixture_fixture
    assert_view_identical(ds)


@pytest.mark.slow
def test_qm3_view_identical(qm3_fixture):
    client, ds = qm3_fixture
    assert_view_identical(ds)


@pytest.mark.slow
def test_s22_view_identical(s22_fixture):
    client, ds = s22_fixture
    assert_view_identical(ds)


@pytest.mark.slow
def test_view_download_remote(s22_fixture):
    _, ds = s22_fixture

    ds.data.__dict__["view_url_hdf5"] = "https://github.com/mattwelborn/QCArchiveViews/raw/master/S22/latest.hdf5"
    ds.data.__dict__["view_metadata"] = {
        "blake2b_checksum": "f9d537a982f63af0c500753b0e4779604252b9289fa26b6d83b657bf6e1039f1af2e44bbc819001fb857f1602280892b48422b8649cea786cdec3d2eb73412a9"
    }
    ds.download()  # 800 kb

    _, dsgz = s22_fixture

    dsgz.data.__dict__["view_url_hdf5"] = "https://github.com/mattwelborn/QCArchiveViews/raw/master/S22/latest.hdf5.gz"
    dsgz.data.__dict__["view_metadata"] = {
        "blake2b_checksum": "f9d537a982f63af0c500753b0e4779604252b9289fa26b6d83b657bf6e1039f1af2e44bbc819001fb857f1602280892b48422b8649cea786cdec3d2eb73412a9"
    }
    dsgz.download()  # 90 kb

    assert ds._view.hash() == dsgz._view.hash()


def test_view_download_mock(gradient_dataset_fixture, tmp_path_factory):
    try:
        import requests_mock
    except ImportError:
        pytest.skip("Missing request_mock")

    client, ds = gradient_dataset_fixture

    with requests_mock.Mocker(real_http=True) as m:
        path = pathlib.Path(tmp_path_factory.mktemp("test_collections"), "ds_gradient_remote.hdf5")
        ds.to_file(path, "hdf5")

        fake_url = "https://qcarchiveviews.com/gradient_ds.h5"
        ds.data.__dict__["view_url_hdf5"] = fake_url
        assert ds.data.id == ds.save()

        with open(path, "rb") as f:
            m.get(fake_url, body=f)
            ds = client.get_collection("Dataset", ds.name)
            ds.download(verify=False)

            # Check main functions run
            ds.get_entries()
            ds.list_values()

            with check_requests_monitor(client, "molecule", request_made=False):
                ds.get_molecules()

            with check_requests_monitor(client, "record", request_made=False):
                ds.get_values()

            ds.data.__dict__["view_metadata"] = {"blake2b_checksum": "badhash"}
            with pytest.raises(ValueError):
                ds.download(verify=True)


def test_gradient_dataset_plaintextview_write(gradient_dataset_fixture, tmpdir):
    _, ds = gradient_dataset_fixture
    ds.to_file(tmpdir / "test.tar.gz", "plaintext")


def test_contributed_dataset_plaintextview_write(contributed_dataset_fixture, tmpdir):
    _, ds = contributed_dataset_fixture
    ds.to_file(tmpdir / "test.tar.gz", "plaintext")


@pytest.mark.slow
def test_s22_dataset_plaintextview_write(s22_fixture, tmpdir):
    _, ds = s22_fixture
    ds.to_file(tmpdir / "test.tar.gz", "plaintext")


@pytest.mark.slow
def test_qm3_dataset_plaintextview_write(qm3_fixture, tmpdir):
    _, ds = qm3_fixture
    ds.to_file(tmpdir / "test.tar.gz", "plaintext")


def test_reactiondataset_dftd3_dataset_plaintextview_write(reactiondataset_dftd3_fixture_fixture, tmpdir):
    _, ds = reactiondataset_dftd3_fixture_fixture
    ds.to_file(tmpdir / "test.tar.gz", "plaintext")


@pytest.mark.slow
def test_s22_dataset_get_molecules_subset(s22_fixture):
    _, ds = s22_fixture
    ds.get_molecules(subset="Adenine-Thymine Complex WC")


### Non-dataset tests


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

    ds.add_entry("hooh1", [hooh1], [[0, 1, 2, 3]], [60], attributes={"something": "hooh1"})
    ds.add_entry("hooh2", [hooh2], [[0, 1, 2, 3]], [60], attributes={"something": "hooh2"})

    optimization_spec = {"program": "geometric", "keywords": {"coordsys": "tric"}}
    qc_spec = {"driver": "gradient", "method": "UFF", "basis": None, "keywords": None, "program": "rdkit"}

    ds.add_specification("Spec1", optimization_spec, qc_spec, description="This is a really cool spec")

    ncompute = ds.compute("spec1")
    assert ncompute == 2
    assert ds.status("Spec1")["Spec1"].sum() == 2

    ds.save()

    fractal_compute_server.await_services(max_iter=1)

    # Check status
    ds.query("Spec1", force=True)
    status_basic = ds.status()
    assert status_basic.loc["RUNNING", "Spec1"] == 2
    status_spec = ds.status("Spec1")
    assert status_basic.loc["RUNNING", "Spec1"] == 2

    status_detail = ds.status("Spec1", detail=True)
    assert status_detail.loc["hooh2", "Complete Tasks"] == 1
    assert status_detail.loc["hooh2", "Total Points"] == 6

    # List of length 1 with detail=True
    status_detail = ds.status(["Spec1"], detail=True)
    assert status_detail.loc["hooh2", "Complete Tasks"] == 1
    assert status_detail.loc["hooh2", "Total Points"] == 6

    fractal_compute_server.await_services(max_iter=7)

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
            assert pytest.approx(ds.df.loc[row, spec].get_final_energies(60), 1.0e-5) == 0.0007991272305133664

    assert ds.status().loc["COMPLETE", "Spec1"] == 2
    assert ds.status(collapse=False).loc["hooh1", "Spec1"] == "COMPLETE"

    assert ds.counts("hooh1").loc["hooh1", "Spec1"] > 5
    assert ds.counts("hooh1", specs="spec1", count_gradients=True).loc["hooh1", "Spec1"] > 30


def test_dataset_list_keywords(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    kw1 = ptl.models.KeywordSet(values={"foo": True})
    kw2 = ptl.models.KeywordSet(values={"foo": False})
    kw3 = ptl.models.KeywordSet(values={"foo": 13})
    ds = ptl.collections.Dataset("test_dataset_list_keywords", client)

    ds.add_keywords(alias="p1_k1", program="p1", keyword=kw1)
    ds.add_keywords(alias="p1_k2", program="p1", keyword=kw2, default=True)
    ds.add_keywords(alias="p2_k3", program="p2", keyword=kw3)
    ds.add_keywords(alias="p3_k1", program="p3", keyword=kw1, default=True)
    ds.save()

    res = ds.list_keywords().reset_index().drop("id", axis=1)
    ref = pd.DataFrame(
        {
            "program": ["p1", "p1", "p2", "p3"],
            "keywords": ["p1_k1", "p1_k2", "p2_k3", "p3_k1"],
            "default": [False, True, False, True],
        }
    )
    assert df_compare(res, ref), res


@testing.using_geometric
@testing.using_rdkit
def test_optimization_dataset(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    ds = ptl.collections.OptimizationDataset("testing", client=client)

    opt_spec = {"program": "geometric"}
    qc_spec = {"driver": "gradient", "method": "UFF", "program": "rdkit"}

    ds.add_specification("test", opt_spec, qc_spec, protocols={"trajectory": "final"})
    ds.add_specification("test2", opt_spec, qc_spec, protocols={"trajectory": "initial_and_final"})

    hooh1 = ptl.data.get_molecule("hooh.json")
    hooh2 = hooh1.copy(update={"geometry": hooh1.geometry + np.array([0, 0, 0.2])})

    ds.add_entry("hooh1", hooh1)
    ds.add_entry("hooh1-2", hooh1)
    ds.add_entry("hooh2", hooh2)

    ds.compute("test")
    ds.compute("test2", subset=["hooh1"])
    fractal_compute_server.await_results()

    ds.query("test")
    ds.query("test2")

    status = ds.status()
    assert status.loc["COMPLETE", "test"] == 3
    assert status.loc["COMPLETE", "test2"] == 1

    status_spec = ds.status(["test", "test2"])
    assert status_spec.loc["COMPLETE", "test"] == 3
    assert status_spec.loc["COMPLETE", "test2"] == 1

    counts = ds.counts()
    assert counts.loc["hooh1", "test"] == 9
    assert np.isnan(counts.loc["hooh2", "test2"])
    assert len(ds.df.loc["hooh1", "test"].trajectory) == 1

    final_energy = 0.00011456853977485626
    for idx, row in ds.df["test"].items():
        assert pytest.approx(row.get_final_energy(), abs=1.0e-5) == final_energy

    opt = ds.get_record("hooh1", "test")
    assert pytest.approx(opt.get_final_energy(), abs=1.0e-5) == final_energy


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

    # Test detail status
    fractal_compute_server.await_services(max_iter=1)
    status_detail = ds.status("test", detail=True)
    assert status_detail.loc["hooh1", "Complete Tasks"] == 1

    # Check completions
    fractal_compute_server.await_services()

    ds.query("test")
    assert ds.get_record("hooh1", "test").status == "COMPLETE"


@pytest.mark.parametrize("ds_class", [ptl.collections.Dataset, ptl.collections.ReactionDataset])
def test_get_collection_no_records(ds_class, fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)
    ds = ds_class(f"tnr_{ds_class.__name__}", client=client)
    ds.save()

    ds = client.get_collection(ds_class.__name__, ds.name)
    assert ds.data.records is None


def test_gradient_dataset_lazy_entries_values(gradient_dataset_fixture):
    client, ds = gradient_dataset_fixture

    ds._clear_cache()
    assert ds.data.records is None
    assert ds.data.contributed_values is None

    ds.get_values(subset=["He1"], basis="3-21g")
    assert (~ds.df.isna()).sum().sum() == 1
    if not ds._use_view():
        assert ds.data.records is not None
    else:
        assert ds.data.records is None


def test_get_collection_no_records_ds(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)
    ds = ptl.collections.Dataset("tnr_test", client=client)
    ds.add_entry("He1", ptl.Molecule.from_data("He -1 0 0\n--\nHe 0 0 1"))
    ds.save()

    ds = client.get_collection("dataset", ds.name)
    assert ds.data.records is None
    ds.get_entries()
    assert len(ds.data.records) == 1
    assert ds.data.records[0].name == "He1"


def test_list_collection_group(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    ds1 = ptl.collections.Dataset(name="tlco_ds1", client=client)
    ds1.save()
    assert (client.list_collections().reset_index().name == "tlco_ds1").any()

    ds2 = ptl.collections.ReactionDataset(name="tlco_ds2", client=client, group="group1")
    ds2.save()
    assert not (client.list_collections().reset_index().name == "tlco_ds2").any()

    assert (client.list_collections(group=None).reset_index().name == "tlco_ds1").any()
    assert (client.list_collections(group=None).reset_index().name == "tlco_ds2").any()


def test_list_collection_visibility(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)
    ds1 = ptl.collections.GridOptimizationDataset(name="tlcv_ds1", client=client, visibility=False)
    ds2 = ptl.collections.GridOptimizationDataset(name="tlcv_ds2", client=client, visibility=True)
    ds3 = ptl.collections.GridOptimizationDataset(name="tlcv_ds3", client=client)
    ds1.save()
    ds2.save()
    ds3.save()

    names = list(client.list_collections().reset_index().name)
    assert "tlcv_ds1" not in names
    assert "tlcv_ds2" in names
    assert "tlcv_ds3" in names

    names = list(client.list_collections(show_hidden=True).reset_index().name)
    assert "tlcv_ds1" in names
    assert "tlcv_ds2" in names
    assert "tlcv_ds3" in names


def test_collection_metadata(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    ds = ptl.collections.Dataset("test_collection_metadata", client=client)
    ds.data.metadata["data_points"] = 133_885
    ds.save()

    assert client.get_collection("dataset", ds.name).data.metadata["data_points"] == 133_885


def test_list_collection_tags(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    ptl.collections.Dataset("test_list_collection_tags_1", client=client, tags=[]).save()
    ptl.collections.Dataset("test_list_collection_tags_2", client=client, tags=["t1"]).save()
    ptl.collections.Dataset("test_list_collection_tags_3", client=client, tags=["t2"]).save()
    ptl.collections.Dataset("test_list_collection_tags_4", client=client, tags=["t1", "t2"]).save()

    names = list(client.list_collections().reset_index().name)
    assert "test_list_collection_tags_1" in names
    assert "test_list_collection_tags_2" in names
    assert "test_list_collection_tags_3" in names
    assert "test_list_collection_tags_4" in names

    names = list(client.list_collections(tag="t1").reset_index().name)
    assert "test_list_collection_tags_1" not in names
    assert "test_list_collection_tags_2" in names
    assert "test_list_collection_tags_3" not in names
    assert "test_list_collection_tags_4" in names

    names = list(client.list_collections(tag=["t1"]).reset_index().name)
    assert "test_list_collection_tags_1" not in names
    assert "test_list_collection_tags_2" in names
    assert "test_list_collection_tags_3" not in names
    assert "test_list_collection_tags_4" in names

    names = list(client.list_collections(tag=["t1", "t2"]).reset_index().name)
    assert "test_list_collection_tags_1" not in names
    assert "test_list_collection_tags_2" in names
    assert "test_list_collection_tags_3" in names
    assert "test_list_collection_tags_4" in names


def test_delete_collection(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)
    client.list_collections()

    ds = ptl.collections.Dataset("test_delete_collection", client=client)
    ds.save()

    dsid = ds.data.id

    client.delete_collection("dataset", ds.name)
    assert ds.name.lower() not in client.list_collections(collection_type="dataset", aslist=True)

    # Fails at get_collection
    with pytest.raises(KeyError):
        client.delete_collection("dataset", ds.name)

    # Test DELETE failure specifically
    with pytest.raises(IOError):
        client._automodel_request(f"collection/{dsid}", "delete", payload={"meta": {}}, full_return=True)
