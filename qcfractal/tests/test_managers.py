"""
Explicit tests for queue manipulation.
"""

import logging
import pytest

import qcfractal.interface as portal
from qcfractal import testing, queue
from qcfractal.testing import test_server


@pytest.fixture(scope="module")
def compute_manager_fixture(test_server):

    client = portal.FractalClient(test_server.get_address())

    # Build Fireworks test server and manager
    fireworks = pytest.importorskip("fireworks")
    logging.basicConfig(level=logging.CRITICAL, filename="/tmp/fireworks_logfile.txt")

    lpad = fireworks.LaunchPad(name="fw_testing_manager", logdir="/tmp/", strm_lvl="CRITICAL")
    lpad.reset(None, require_password=False)

    manager = queue.QueueManager(client, lpad)

    yield client, test_server, manager

    # Cleanup and reset
    lpad.reset(None, require_password=False)
    logging.basicConfig(level=None, filename=None)


@testing.using_rdkit
def test_queue_manager_single(compute_manager_fixture):
    client, server, manager = compute_manager_fixture

    # Add compute
    hooh = portal.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", [hooh.to_json()])

    # Force manager compute and get results
    manager.await_results()
    ret = client.get_results()
    assert len(ret) == 1
