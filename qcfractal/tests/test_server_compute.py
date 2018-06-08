"""
Tests the server compute capabilities.
"""

import qcfractal.interface as qp
import qcfractal as qf
import qcfractal.testing as testing
from qcfractal.queue_handlers import build_queue

import qcengine

test_db = testing.test_database

@testing.using_psi4
@testing.using_dask
def test_queue_stack_dask(test_db):

    from dask.distributed import Client, LocalCluster

    cluster = LocalCluster(n_workers=1, threads_per_worker=1)
    client = Client(cluster)


    # Add a hydrogen molecule
    hydrogen = qp.Molecule([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")
    mol_ret = test_db.add_molecules({"hydrogen": hydrogen.to_json()})

    nanny, scheduler = build_queue("dask", client, test_db)

    compute = {
        "schema_name": "qc_schema_input",
        "schema_version": 1,
        # "molecule": mol_ret["data"]["hydrogen"],
        "molecule": hydrogen.to_json(),
        "driver": "energy",
        "model": {"method": "HF", "basis":"sto-3g"},
        "keywords": {"scf_type": "df"}
    }
    # help(client.submit)
    fut = client.submit(qcengine.compute, compute, "psi4")

    print(fut)

    print(fut.result())

    client.close()