"""
Explicit tests for queue manipulation.
"""

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import fractal_compute_server


@testing.using_rdkit
def test_queue_error(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    hooh = portal.data.get_molecule("hooh.json").json_dict()
    del hooh["connectivity"]

    compute_ret = client.add_compute("rdkit", "UFF", "", "energy", None, hooh)

    # Pull out a special iteration on the queue manager
    fractal_compute_server.update_tasks()
    assert len(fractal_compute_server.list_current_tasks()) == 1

    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    db = fractal_compute_server.objects["storage_socket"]
    queue_ret = db.get_queue(status="ERROR")["data"]
    result = db.get_results_by_id(compute_ret.ids)['data'][0]

    assert len(queue_ret) == 1
    assert "connectivity graph" in queue_ret[0]["error"]["error_message"]
    assert result['status'] == 'ERROR'

    # Force a complete mark and test
    fractal_compute_server.objects["storage_socket"].queue_mark_complete([queue_ret[0]["id"]])
    result = db.get_results_by_id(compute_ret.ids)['data'][0]
    assert result['status'] == 'COMPLETE'


@testing.using_rdkit
def test_queue_duplicate_compute(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    hooh = portal.data.get_molecule("hooh.json").json_dict()
    mol_ret = client.add_molecules([hooh])

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret)
    assert len(ret.ids) == 1
    assert len(ret.existing) == 0

    # Pull out fireworks launchpad and queue nanny
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret)
    assert len(ret.ids) == 1
    assert len(ret.existing) == 1


@testing.using_rdkit
def test_queue_compute_mixed_molecule(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    mol1 = portal.Molecule.from_data("He 0 0 0\nHe 0 0 2.1")
    mol_ret = client.add_molecules([mol1])

    mol2 = portal.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [mol1, mol2, "bad_id"], return_full=True)
    assert len(ret.data.ids) == 3
    assert ret.data.ids[2] is None
    assert len(ret.data.submitted) == 2
    assert len(ret.data.existing) == 0

    # Pull out fireworks launchpad and queue nanny
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [mol_ret[0], "bad_id2"])
    assert len(ret.ids) == 2
    assert ret.ids[1] is None
    assert len(ret.submitted) == 0
    assert len(ret.existing) == 1


@testing.using_rdkit
@testing.using_geometric
def test_queue_duplicate_procedure(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    hooh = portal.data.get_molecule("hooh.json").json_dict()
    mol_ret = client.add_molecules([hooh])

    geometric_options = {
        "keywords": None,
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit"
        },
    }

    ret = client.add_procedure("optimization", "geometric", geometric_options, [mol_ret[0], "bad_id"])
    assert len(ret.ids) == 2
    assert ret.ids[1] is None
    assert len(ret.submitted) == 1
    assert len(ret.existing) == 0

    # Pull out fireworks launchpad and queue nanny
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret2 = client.add_procedure("optimization", "geometric", geometric_options, ["bad_id", hooh])
    assert len(ret2.ids) == 2
    assert ret2.ids[0] is None
    assert len(ret2.submitted) == 0
    assert len(ret2.existing) == 1

    assert ret.ids[0] == ret2.ids[1]
