"""
Contains testing infrastructure for QCFractal.
"""

import pytest


def pytest_addoption(parser):
    """
    Additional PyTest CLI flags to add
    See `pytest_collection_modifyitems` for handling and `pytest_configure` for adding known in-line marks.
    """

    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


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


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: Mark a given test as slower than most other tests")
    config.addinivalue_line("markers", "full: Mark a given test as a full end-to-end test")


def pytest_unconfigure(config):
    pass
