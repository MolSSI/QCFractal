"""
Tests the DQM Server class
"""

import qcfractal as ds
import qcfractal.interface as qp

import pytest
import pymongo
import threading
from contextlib import contextmanager
from tornado.ioloop import IOLoop
import requests

server_port = 8888
server_address = "http://localhost:" + str(server_port) + "/"

mol_api_addr = server_address + "molecule"

@contextmanager
def pristine_loop():
    """
    Builds a clean IOLoop for using as a background request.
    Courtesy of Dask Distributed
    """
    IOLoop.clear_instance()
    IOLoop.clear_current()
    loop = IOLoop()
    loop.make_current()
    assert IOLoop.current() is loop
    try:
        yield loop
    finally:
        try:
            loop.close(all_fds=True)
        except ValueError:
            pass
        IOLoop.clear_instance()
        IOLoop.clear_current()


@pytest.fixture(scope="module")
def server(request):
    """
    Builds a server instance with the event loop running in a thread.
    """

    db_name = "dqm_local_values_test"

    with pristine_loop() as loop:
        # Clean and re-init the databse, manually handle IOLoop (no start/stop needed)
        print("")
        server = ds.DQMServer(port=server_port, db_project_name=db_name, io_loop=loop)
        server.db.client.drop_database(server.db._project_name)
        server.db.init_database()

        # Add the IOloop to a thread daemon
        thread = threading.Thread(target=loop.start,
                                  name="test IOLoop")
        thread.daemon = True
        thread.start()
        loop_started = threading.Event()
        loop.add_callback(loop_started.set)
        loop_started.wait()

        # Yield the server instance
        yield server

        # Cleanup
        loop.add_callback(loop.stop)
        thread.join(timeout=5)

def test_molecule_socket(server):

    water = qp.data.get_molecule("water_dimer_minima.psimol")

    # Add a molecule
    r = requests.post(mol_api_addr, json=water.to_json())
    assert r.status_code == 200

    pdata = r.json()
    assert pdata.keys() == {"errors", "ids", "nInserted", "success"}

    # Retrieve said molecule
    r = requests.get(mol_api_addr, json={"ids": pdata["ids"], "index": "id"})
    assert r.status_code == 200

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])

    # Retrieve said molecule via hash
    r = requests.get(mol_api_addr, json={"ids": water.get_hash(), "index": "hash"})
    assert r.status_code == 200

    gdata = r.json()
    assert isinstance(gdata["data"], list)

    assert water.compare(gdata["data"][0])