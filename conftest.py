"""
Contains testing infrastructure for QCFractal.
"""

import pytest


def pytest_addoption(parser):
    """
    Additional PyTest CLI flags to add

    See `pytest_collection_modifyitems` for handling and `pytest_configure` for adding known in-line marks.

    Note that --client-encoding and --fractal-uri are added by the qcarchivetesting pytest plugin,
    since they are used by fixtures distributed in that package (this file is not distributed
    anywhere, so options defined here are not available to other projects).
    """

    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_collection_modifyitems(config, items):
    runslow = config.getoption("--runslow")

    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords and not runslow:
            item.add_marker(skip_slow)


def pytest_unconfigure(config):
    pass
