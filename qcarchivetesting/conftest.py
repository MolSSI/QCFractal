"""
Contains testing infrastructure for QCFractal.
"""

import pytest

from qcportal import PortalClient


def pytest_addoption(parser):
    """
    Additional PyTest CLI flags to add

    See `pytest_collection_modifyitems` for handling and `pytest_configure` for adding known in-line marks.
    """

    parser.addoption(
        "--fractal-uri", action="store", help="URI of the fractal instance to run the tests against", required=True
    )


@pytest.fixture(scope="function")
def fulltest_client(pytestconfig):
    """
    A portal client used for full end-to-end tests
    """

    uri = pytestconfig.getoption("--fractal-uri")

    if uri == "snowflake":
        from qcfractal.snowflake import FractalSnowflake

        s = FractalSnowflake()
        yield s.client()
        s.stop()

    else:
        yield PortalClient(address=uri)
