"""
Tests the server compute capabilities.
"""

import qcfractal.interface as qp
import qcfractal as qf

from qcfractal.queue_handlers import build_queue
from qcfractal import testing
import qcengine
import requests

dask_server = testing.test_dask_server
scheduler_api_addr = testing.test_server_address + "scheduler"

@testing.using_psi4
@testing.using_dask
def test_queue_stack_dask(dask_server):


    # Add a hydrogen molecule
    hydrogen = qp.Molecule([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")
    db = dask_server.objects["db_socket"]
    mol_ret = db.add_molecules({"hydrogen": hydrogen.to_json()})

    # Add compute
    compute = {
        "schema_name": "qc_schema_input",
        "schema_version": 1,
        "molecule": mol_ret["data"]["hydrogen"],
        "driver": "energy",
        "model": {"method": "HF", "basis":"sto-3g"},
        "keywords": {"scf_type": "df"}
    }

    print("here")
    r = requests.get(testing.test_server_address + "molecule", json={"meta":{}, "data": [mol_ret["data"]["hydrogen"]]})
    # r = requests.post(scheduler_api_addr, json={"meta": {}, "data":[compute]})
    print("here")
    assert r.status_code == 200


    # # Manually handle the compute
    # client = self.objects["queue_socket"]
    # ret = client.futures[0].results()
    # print(ret)





