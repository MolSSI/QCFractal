"""
Explicit tests for queue manipulation.
"""

import qcfractal.interface as portal
from qcfractal.testing import fireworks_server_fixture as fw_server
from qcfractal.testing import fractal_compute_server
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
    nanny = fw_server.objects["queue_manager"]

    # Push jobs to nanny and check
    nanny.update()
    assert len(lpad.get_fw_ids()) == 1
    assert len(nanny.list_current_tasks()) == 1

    # Await results and ensure that it is clean
    nanny.await_results()
    assert len(lpad.get_fw_ids()) == 0
    assert len(nanny.list_current_tasks()) == 0


@testing.using_rdkit
def test_queue_error(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address())

    hooh = portal.data.get_molecule("hooh.json").to_json()
    del hooh["connectivity"]
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["hooh"])
    queue_id = ret["submitted"][0]

    # Pull out fireworks launchpad and queue nanny
    nanny = fractal_compute_server.objects["queue_manager"]

    nanny.update()
    assert len(nanny.list_current_tasks()) == 1

    nanny.await_results()
    assert len(nanny.list_current_tasks()) == 0

    db = fractal_compute_server.objects["storage_socket"]
    ret = db.get_queue({"status": "ERROR"})["data"]

    assert len(ret) == 1
    assert "connectivity graph" in ret[0]["error"]
    fractal_compute_server.objects["storage_socket"].queue_mark_complete([(queue_id, "completed_pointer")])


@testing.using_rdkit
def test_queue_duplicate_compute(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address())

    hooh = portal.data.get_molecule("hooh.json").to_json()
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["hooh"])
    assert len(ret["submitted"]) == 1
    assert len(ret["completed"]) == 0

    # Pull out fireworks launchpad and queue nanny
    nanny = fractal_compute_server.objects["queue_manager"]
    nanny.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["hooh"])
    assert len(ret["submitted"]) == 0
    assert len(ret["completed"]) == 1

@testing.using_rdkit
@testing.using_geometric
def test_queue_duplicate_procedure(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address())

    hooh = portal.data.get_molecule("hooh.json").to_json()
    mol_ret = client.add_molecules({"hooh": hooh})

    geometric_options = {
        "options": "none",
        "qc_meta": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "options": "none",
            "program": "rdkit"
        },
    }

    ret = client.add_procedure("optimization", "geometric", geometric_options, mol_ret["hooh"])
    assert len(ret["submitted"]) == 1
    assert len(ret["completed"]) == 0

    # Pull out fireworks launchpad and queue nanny
    nanny = fractal_compute_server.objects["queue_manager"]
    nanny.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_procedure("optimization", "geometric", geometric_options, mol_ret["hooh"])
    assert len(ret["submitted"]) == 0
    assert len(ret["completed"]) == 1


@testing.using_rdkit
def test_queue_duplicate_submissions(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address())

    he2 = portal.data.get_molecule("helium_dimer.json").to_json()
    mol_ret = client.add_molecules({"he2": he2})

    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["he2"])
    assert len(ret["submitted"]) == 1
    assert len(ret["completed"]) == 0
    assert len(ret["queue"]) == 0
    queue_id = ret["submitted"][0]

    # Do not compute, add duplicate
    ret = client.add_compute("rdkit", "UFF", "", "energy", "none", mol_ret["he2"])
    assert len(ret["submitted"]) == 0
    assert len(ret["completed"]) == 0
    assert len(ret["queue"]) == 1
    assert ret["queue"][0] == queue_id

    # Cleanup
    fractal_compute_server.objects["storage_socket"].queue_mark_complete([(queue_id, "output")])

