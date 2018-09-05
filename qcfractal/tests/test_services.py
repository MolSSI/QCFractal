"""
Tests the on-node procedures compute capabilities.
"""

from qcfractal import testing
# Pytest Fixture import
from qcfractal.testing import dask_server_fixture
import pytest

import qcfractal.interface as portal


### Tests the compute queue stack
@testing.using_torsiondrive
@testing.using_geometric
@testing.using_rdkit
def test_service_torsiondrive(dask_server_fixture):

    client = portal.FractalClient(dask_server_fixture.get_address())

    # Add a HOOH
    hooh = portal.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules({"hooh": hooh})

    # Geometric options
    torsiondrive_options = {
        "torsiondrive_meta": {
           "dihedrals": [[0, 1, 2, 3]],
           "grid_spacing": [90]
        },
        "geometric_meta": {
            "coordsys": "tric"
        },
        "qc_meta": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "options": "none",
            "program": "rdkit",
        },
    }

    ret = client.add_service("torsiondrive", [mol_ret["hooh"]], torsiondrive_options)
    compute_key = ret[0]

    # Manually handle the compute
    nanny = dask_server_fixture.objects["queue_nanny"]
    nanny.await_services(max_iter=5)
    assert len(nanny.list_current_tasks()) == 0

    # Get a TorsionDriveORM result and check data
    result = client.get_service(compute_key)[0]
    assert isinstance(str(result), str)  # Check that repr runs

    assert pytest.approx(0.002597541340221565, 1e-5) == result.final_energies(0)
    assert pytest.approx(0.000156553761859276, 1e-5) == result.final_energies(90)
    assert pytest.approx(0.000156553761859271, 1e-5) == result.final_energies(-90)
    assert pytest.approx(0.000753492556057886, 1e-5) == result.final_energies(180)
