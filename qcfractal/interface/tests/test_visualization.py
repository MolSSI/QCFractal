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
    _has_ploty is False, reason="Not detecting module 'plotly'. Install package if necessary to enable tests."
)


def live_fractal_or_skip():
    """
    Ensure Fractal live connection can be made
    First looks for a local staging server, then tries QCArchive.
    """
    try:
        return portal.FractalClient("localhost:7777", verify=False)
    except (requests.exceptions.ConnectionError, ConnectionRefusedError):
        print("Failed to connect to localhost")
        try:
            requests.get("https://api.qcarchive.molssi.org:443", json={}, timeout=5)
            return portal.FractalClient()
        except (requests.exceptions.ConnectionError, ConnectionRefusedError):
            return pytest.skip("Could not make a connection to central Fractal server")


@pytest.fixture
def S22Fixture():

    # Connect to the primary database
    client = live_fractal_or_skip()

    S22 = client.get_collection("ReactionDataset", "S22")

    return client, S22


@using_plotly
@pytest.mark.parametrize("kind", ["bar", "violin"])
def test_plot_dataset(S22Fixture, kind):

    client, S22 = S22Fixture

    fig = S22.visualize(
        method=["b2plyp", "pbe"], basis=["def2-svp", "def2-TZVP"], return_figure=True, bench="S22a", kind=kind
    ).to_dict()
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
        groupby=groupby,
    ).to_dict()
    assert "S22" in fig["layout"]["title"]["text"]


@using_plotly
def test_plot_qca_examples(S22Fixture):
    """ Tests plotting examples from QCArchiveExamples/basic_examples/reaction_dataset.ipynb"""
    client, S22 = S22Fixture
    fig = S22.visualize(method=["B3LYP", "B3LYP-D3", "B3LYP-D3M"], basis=["def2-tzvp"], groupby="D3").to_dict()
    assert "S22" in fig["layout"]["title"]["text"]
    fig = S22.visualize(
        method=["B3LYP", "B3LYP-D3", "B2PLYP", "B2PLYP-D3"], basis="def2-tzvp", groupby="D3", kind="violin"
    )
    assert "S22" in fig["layout"]["title"]["text"]


### Test TorsionDriveDataset scans


@pytest.fixture
def TDDSFixture():
    pytest.skip("Database and tests are out of sync, fixed in 6.1 release.")

    # Connect to the primary database
    client = live_fractal_or_skip()
    TDDs = client.get_collection("TorsionDriveDataset", "OpenFF Fragmenter Phenyl Benchmark")

    return client, TDDs


@using_plotly
def test_plot_torsiondrive_dataset(TDDSFixture):
    client, ds = TDDSFixture

    ds.visualize("[CH3:4][O:3][c:2]1[cH:1]cccc1", ["B3LYP-D3", "UFF"], units="kJ / mol", return_figure=True)
    ds.visualize(
        ["[CH3:4][O:3][c:2]1[cH:1]cccc1", "[CH3:4][O:3][c:2]1[cH:1]ccnc1"], "UFF", relative=False, return_figure=True
    )


@using_plotly
def test_plot_torsiondrive_dataset_measured(TDDSFixture):
    client, ds = TDDSFixture

    ds.visualize(
        "[CH3:4][O:3][c:2]1[cH:1]cccc1", "B3LYP-D3", units="kJ / mol", use_measured_angle=True, return_figure=True
    )
