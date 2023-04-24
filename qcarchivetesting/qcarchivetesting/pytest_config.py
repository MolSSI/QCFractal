"""
Contains testing infrastructure for QCFractal.
"""

# This file is normally named conftest.py, but doing so
# runs into issues with pytest automatically finding this, but also
# having it be found through the pyproject.toml entry point

import pytest


def pytest_addoption(parser):
    """
    Additional PyTest CLI flags to add

    See `pytest_collection_modifyitems` for handling and `pytest_configure` for adding known in-line marks.
    """

    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption("--client-encoding", type=str, default="application/json", help="set client encoding to test")
    parser.addoption(
        "--fractal-uri", type=str, default="snowflake", help="URI of the fractal instance to run full tests against"
    )


def pytest_collection_modifyitems(config, items):
    """
    Handle test triggers based on the CLI flags
    Use decorators:
    @pytest.mark.slow
    """
    runslow = config.getoption("--runslow")

    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords and not runslow:
            item.add_marker(skip_slow)


def pytest_unconfigure(config):
    pass
