"""
Explicit tests for queue manipulation.
"""

import qcfractal.interface as portal
from qcfractal.testing import fireworks_server_fixture as fw_server
from qcfractal.testing import dask_server_fixture as fractal_compute_server
from qcfractal import testing


@testing.using_rdkit
@testing.using_fireworks
def test_queue_fireworks_cleanup(fw_server):

    client = portal.FractalClient(fw_server.get_address())

    hooh = portal.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["hooh"])

    # Pull out fireworks launchpad and queue nanny
    lpad = fw_server.objects["queue_socket"]
    nanny = fw_server.objects["queue_nanny"]

    assert len(lpad.get_fw_ids()) == 1
    assert len(nanny.list_current_tasks()) == 1

    nanny.await_results()

    assert len(lpad.get_fw_ids()) == 0
    assert len(nanny.list_current_tasks()) == 0


@testing.using_rdkit
@testing.using_fireworks
def test_queue_error(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address())

    hooh = portal.data.get_molecule("hooh.json").to_json()
    del hooh["connectivity"]
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["hooh"])

    # Pull out fireworks launchpad and queue nanny
    nanny = fractal_compute_server.objects["queue_nanny"]

    assert len(nanny.list_current_tasks()) == 1
    nanny.await_results()
    assert len(nanny.list_current_tasks()) == 0

    db = fractal_compute_server.objects["storage_socket"]
    ret = db.get_queue([{"status": "ERROR"}])["data"]

    # print(ret[0]["error"])
    assert len(ret) == 1
    assert "Distributed Worker Error" in ret[0]["error"]


@testing.using_rdkit
@testing.using_fireworks
def test_queue_duplicate_single(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address())

    hooh = portal.data.get_molecule("hooh.json").to_json()
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["hooh"])
    assert len(ret["submitted"]) == 1
    assert len(ret["duplicates"]) == 0

    # Pull out fireworks launchpad and queue nanny
    nanny = fractal_compute_server.objects["queue_nanny"]
    nanny.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["hooh"])
    assert len(ret["submitted"]) == 0
    assert len(ret["duplicates"]) == 1
