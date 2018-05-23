"""
Tests the interface portal adapter to the REST API
"""

import qcfractal as qf
import qcfractal.interface as qp
from qcfractal.testing import test_server, test_server_address

import pytest

# All tests should import test_server, but not use it
# Make PyTest aware that this module needs the server


def test_molecule_portal(test_server):

    portal = qp.QCPortal(test_server_address)

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Test add
    ret = portal.add_molecules({"water": water})

    # Test get
    get_mol = portal.get_molecules(ret["water"], index="id")

    assert water.compare(get_mol[0])

def test_options_portal(test_server):

    portal = qp.QCPortal(test_server_address)

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Test add
    ret = portal.add_molecules({"water": water})

    # Test get
    get_mol = portal.get_molecules(ret["water"], index="id")

    assert water.compare(get_mol[0])