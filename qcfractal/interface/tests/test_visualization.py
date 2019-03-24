"""
Tests for visualization code
"""

import pytest

from . import portal

try:
    import plotly
    _has_ploty = True
except ModuleNotFoundError:
    _has_ploty = False

using_plotly = pytest.mark.skipif(
    _has_ploty is False, reason="Not detecting module 'plotly'. Install package if necessary to enable tests.")


@pytest.fixture
def S22Fixture():

    # Connect to the primary database
    client = portal.FractalClient()

    S22 = client.get_collection("ReactionDataset", "S22")

    return (client, S22)


@using_plotly
@pytest.mark.parametrize("kind", ["violin", "bar"])
def test_dataset_plot(S22Fixture, kind):

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
def test_dataset_groupby_plot(S22Fixture, kind, groupby):

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
    client = portal.FractalClient()
    TDDs = client.get_collection("TorsionDriveDataset", "OpenFF Fragmenter Phenyl Benchmark")

    return (client, TDDs)


@using_plotly
@pytest.mark.parametrize("measured", [True, False])
def test_torsiondrive_dataset_visualize(TDDSFixture, measured):

    client, ds = TDDSFixture

    client = portal.FractalClient()
    ds = client.get_collection("TorsionDriveDataset", "OpenFF Fragmenter Phenyl Benchmark")

    ds.visualize(
        "c1ccc(cc1)N-[3, 5, 6, 12]", ["b3lyp", "uff"],
        units="kJ / mol",
        use_measured_angle=measured,
        return_figure=True)
    ds.visualize(
        ["c1ccc(cc1)N-[3, 5, 6, 12]", "CCCNc1ccc(cc1)Cl-[1, 4, 9, 8]"],
        "uff",
        use_measured_angle=measured,
        relative=False,
        return_figure=True)