"""
Tests for visualization code
"""

import pytest
import requests

from . import portal

try:
    import plotly
    _has_ploty = True
except ModuleNotFoundError:
    _has_ploty = False

using_plotly = pytest.mark.skipif(
    _has_ploty is False, reason="Not detecting module 'plotly'. Install package if necessary to enable tests.")


def live_fractal_or_skip():
    """Ensure Fractal live connection can be made"""
    try:
        return portal.FractalClient()
    except requests.exceptions.ConnectionError:
        return pytest.skip("Could not make a connection to central Fractal server")


@pytest.fixture
def S22Fixture():

    # Connect to the primary database
    client = live_fractal_or_skip()

    S22 = client.get_collection("ReactionDataset", "S22")

    return (client, S22)


@using_plotly
@pytest.mark.parametrize("kind", ["violin", "bar"])
def test_plot_dataset(S22Fixture, kind):

    client, S22 = S22Fixture

    fig = S22.visualize(
        method=["b2plyp", "b3lyp", "pbe"],
        basis=["def2-svp", "def2-TZVP"],
        return_figure=True,
        bench="S22a",
        kind=kind).to_dict()
    assert "S22" in fig["layout"]["title"]["text"]


@using_plotly
@pytest.mark.parametrize("kind", ["violin", "bar"])
@pytest.mark.parametrize("groupby", ["method", "basis"])
def test_plot_dataset_groupby(S22Fixture, kind, groupby):

    client, S22 = S22Fixture

    fig = S22.visualize(
        method=["b2plyp", "b3lyp"],
        basis=["def2-svp", "def2-TZVP"],
        return_figure=True,
        bench="S22a",
        kind=kind,
        groupby=groupby).to_dict()
    assert "S22" in fig["layout"]["title"]["text"]


### Test TorsionDriveDataset scans


@pytest.fixture
def TDDSFixture():

    # Connect to the primary database
    client = live_fractal_or_skip()
    TDDs = client.get_collection("TorsionDriveDataset", "OpenFF Fragmenter Phenyl Benchmark")

    return (client, TDDs)


@using_plotly
def test_plot_torsiondrive_dataset(TDDSFixture):
    client, ds = TDDSFixture

    ds.visualize("c1ccc(cc1)N-[3, 5, 6, 12]", ["B3LYP-D3", "UFF"], units="kJ / mol", return_figure=True)
    ds.visualize(
        ["c1ccc(cc1)N-[3, 5, 6, 12]", "CCCNc1ccc(cc1)Cl-[1, 4, 9, 8]"], "UFF", relative=False, return_figure=True)


@using_plotly
def test_plot_torsiondrive_dataset_measured(TDDSFixture):
    client, ds = TDDSFixture

    ds.visualize("c1ccc(cc1)N-[3, 5, 6, 12]", "B3LYP-D3", units="kJ / mol", use_measured_angle=True, return_figure=True)
