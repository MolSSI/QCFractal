"""
Explicit tests for queue manipulation.
"""

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import fractal_compute_server


@testing.using_rdkit
def test_queue_error(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    hooh = portal.data.get_molecule("hooh.json").json(as_dict=True)
    del hooh["connectivity"]
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret["hooh"])
    queue_id = ret["submitted"][0]

    # Pull out a special iteration on the queue manager
    fractal_compute_server.update_tasks()
    assert len(fractal_compute_server.list_current_tasks()) == 1

    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    db = fractal_compute_server.objects["storage_socket"]
    ret = db.get_queue({"status": "ERROR"})["data"]

    assert len(ret) == 1
    assert "connectivity graph" in ret[0]["error"]["error_message"]
    fractal_compute_server.objects["storage_socket"].queue_mark_complete([queue_id])


@testing.using_rdkit
def test_queue_duplicate_compute(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    hooh = portal.data.get_molecule("hooh.json").json(as_dict=True)
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret["hooh"])
    assert len(ret["submitted"]) == 1
    assert len(ret["completed"]) == 0

    # Pull out fireworks launchpad and queue nanny
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret["hooh"])
    assert len(ret["submitted"]) == 0
    assert len(ret["completed"]) == 1


@testing.using_rdkit
@testing.using_geometric
def test_queue_duplicate_procedure(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    hooh = portal.data.get_molecule("hooh.json").json(as_dict=True)
    mol_ret = client.add_molecules({"hooh": hooh})

    geometric_options = {
        "options": None,
        "qc_meta": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "options": None,
            "program": "rdkit"
        },
    }

    ret = client.add_procedure("optimization", "geometric", geometric_options, mol_ret["hooh"])
    assert len(ret["submitted"]) == 1
    assert len(ret["completed"]) == 0

    # Pull out fireworks launchpad and queue nanny
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_procedure("optimization", "geometric", geometric_options, mol_ret["hooh"])
    assert len(ret["submitted"]) == 0
    assert len(ret["completed"]) == 1


@testing.using_rdkit
def test_queue_duplicate_submissions(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    he2 = portal.data.get_molecule("helium_dimer.json").json(as_dict=True)
    mol_ret = client.add_molecules({"he2": he2})

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret["he2"])
    assert len(ret["submitted"]) == 1
    assert len(ret["completed"]) == 0
    assert len(ret["queue"]) == 0
    task_id = ret["submitted"][0]

    # Do not compute, add duplicate
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret["he2"])
    assert len(ret["submitted"]) == 0
    assert len(ret["completed"]) == 1
    # assert ret["queue"][0] == task_id
    # assert len(ret["queue"]) == 1
    # assert ret["queue"][0] == task_id

    # Cleanup
    fractal_compute_server.objects["storage_socket"].queue_mark_complete([task_id])
