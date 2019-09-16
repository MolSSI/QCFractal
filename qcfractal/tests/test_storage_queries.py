"""
Tests the on-node procedures compute capabilities.
"""

import copy

import pytest

import qcfractal.interface as ptl
from qcfractal.interface.models import GridOptimizationInput, TorsionDriveInput, Molecule
from qcfractal.testing import fractal_compute_server, recursive_dict_merge, using_geometric, using_rdkit
from qcelemental.util import msgpackext_dumps, msgpackext_loads
import numpy as np


@pytest.fixture(scope="module")
def torsiondrive_fixture(fractal_compute_server):

    # Cannot use this fixture without these services. Also cannot use `mark` and `fixture` decorators
    pytest.importorskip("torsiondrive")
    pytest.importorskip("geometric")
    pytest.importorskip("rdkit")

    client = ptl.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = ptl.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    # Geometric options
    torsiondrive_options = {
        "initial_molecule": mol_ret[0],
        "keywords": {
            "dihedrals": [[0, 1, 2, 3]],
            "grid_spacing": [90]
        },
        "optimization_spec": {
            "program": "geometric",
            "keywords": {
                "coordsys": "tric",
            }
        },
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit",
        },
    } # yapf: disable

    def spin_up_test(**keyword_augments):
        run_service = keyword_augments.pop("run_service", True)

        instance_options = copy.deepcopy(torsiondrive_options)
        recursive_dict_merge(instance_options, keyword_augments)

        inp = TorsionDriveInput(**instance_options)
        ret = client.add_service([inp], full_return=True)

        if ret.meta.n_inserted:  # In case test already submitted
            compute_key = ret.data.ids[0]
            service = client.query_services(procedure_id=compute_key)[0]
            assert 'WAITING' in service['status']

        if run_service:
            fractal_compute_server.await_services()
            assert len(fractal_compute_server.list_current_tasks()) == 0

        return ret.data

    yield spin_up_test, client


def test_torsiondrive_initial_final_molecule(torsiondrive_fixture, fractal_compute_server):
    """ With single initial molecule in torsion proc"""

    spin_up_test, client = torsiondrive_fixture

    ret = spin_up_test()

    # Get a TorsionDriveORM result and check data
    result = client.query_procedures(id=ret.ids)[0]

    assert result.status == "COMPLETE"
    assert isinstance(str(result), str)  # Check that repr runs

    # copy from test_services
    assert pytest.approx(0.002597541340221565, abs=1e-6) == result.get_final_energies(0)
    assert pytest.approx(0.000156553761859276, abs=1e-6) == result.get_final_energies(90)
    assert pytest.approx(0.000156553761859271, abs=1e-6) == result.get_final_energies(-90)
    assert pytest.approx(0.000753492556057886, abs=1e-6) == result.get_final_energies(180)

    assert hasattr(result.get_final_molecules()[(-90, )], "symbols")

    # print(fractal_compute_server.storage.uri)
    # torsion_id = fractal_compute_server.storage.get_procedures(procedure='torsiondrive')['data'][0]['id']
    torsion_id = ret.ids[0]

    r = fractal_compute_server.storage.query('torsiondrive', 'initial_molecules_ids',
                                             torsion_id=torsion_id)

    assert r['meta']['success']
    assert len(r['data']) == 9

    r = fractal_compute_server.storage.query('torsiondrive', 'initial_molecules',
                                             torsion_id=torsion_id)
    assert r['meta']['success']
    assert len(r['data']) == 9
    mol = r['data'][0]

    # Msgpack field
    assert isinstance(msgpackext_loads(mol['mass_numbers']), np.ndarray)  # TODO

    # Sample fields in the molecule dict
    assert all(x in mol.keys()
               for x in ['schema_name', 'symbols', 'geometry',  'molecular_charge'])

    r = fractal_compute_server.storage.query('torsiondrive', 'final_molecules_ids',
                                             torsion_id=torsion_id)

    assert r['meta']['success']
    assert len(r['data']) == 9

    r = fractal_compute_server.storage.query('torsiondrive', 'final_molecules',
                                             torsion_id=torsion_id)
    assert r['meta']['success']
    assert len(r['data']) == 9
    mol = r['data'][0]

    # TODO: can't automatically convert msgpack
    # assert Molecule(**r['data'][0], validate=False, validated=True)


def test_torsiondrive_return_results(torsiondrive_fixture, fractal_compute_server):
    """ With single initial molecule in torsion proc"""

    spin_up_test, client = torsiondrive_fixture

    ret = spin_up_test()
    torsion_id = ret.ids[0]

    r = fractal_compute_server.storage.query('torsiondrive', 'return_results',
                                             torsion_id=torsion_id)
    assert r['meta']['success']
    assert len(r['data'])
    assert all(x in r['data'][0] for x in ['result_id', 'return_result'])


def test_torsiondrive_best_opt_results(torsiondrive_fixture, fractal_compute_server):
    """ Test return best optimization proc results in one query"""

    spin_up_test, client = torsiondrive_fixture

    ret = spin_up_test()
    torsion_id = ret.ids[0]

    torsion = fractal_compute_server.storage.get_procedures(id=torsion_id)['data'][0]

    opt_ids = [torsion['optimization_history'][k][v] for k,v in torsion['minimum_positions'].items()]

    # TODO; is unique values needed?
    opt_ids = set(opt_ids)

    r = fractal_compute_server.storage.query('procedure', 'best_opt_results', opt_ids=opt_ids)

    assert r['meta']['success']
    assert len(r['data']) == len(opt_ids)
    # found = {str(result['opt_id']) for result in r['data']}
    assert  set(r['data'].keys()) == set(map(int, opt_ids))

    # Msgpack field
    res = list(r['data'].values())[0]
    # print('Data[0]: \n', res, '\n')
    # print('Return_results raw:', bytes(res['return_result']))

    assert isinstance(msgpackext_loads(res['return_result']), np.ndarray)


def test_torsiondrive_all_opt_results(torsiondrive_fixture, fractal_compute_server):
    """ Test return best optimization proc results in one query"""

    spin_up_test, client = torsiondrive_fixture

    ret = spin_up_test()
    torsion_id = ret.ids[0]

    torsion = fractal_compute_server.storage.get_procedures(id=torsion_id)['data'][0]

    opt_ids = [torsion['optimization_history'][k][v] for k,v in torsion['minimum_positions'].items()]

    # TODO; is unique values needed?
    opt_ids = set(opt_ids)

    r = fractal_compute_server.storage.query('procedure', 'all_opt_results', opt_ids=opt_ids)

    # print('len of data:', len(r['data']), '\n')

    assert r['meta']['success']
    assert len(r['data']) == len(opt_ids)


    # Msgpack field
    sample_res = r['data'][0]['trajectory_results'][0]
    # print('Return_results raw:', sample_res['return_result'])
    bytes_arr = bytes.fromhex(sample_res['return_result'][2:])  # slice to remove the '\x'
    # print('Return_results bytes.fromhex:', bytes_arr)

    assert isinstance(msgpackext_loads(bytes_arr), np.ndarray)



# def test_service_torsiondrive_get_final_results(torsiondrive_fixture):
#     """Test the get_final_results function for the 1-D case"""
#
#     spin_up_test, client = torsiondrive_fixture
#     ret = spin_up_test()
#
#     # Get a TorsionDriveORM result and check data
#     result = client.query_procedures(id=ret.ids)[0]
#     assert result.status == "COMPLETE"
#
#     final_result_records = result.get_final_results()
#     assert set(final_result_records.keys()) == {(-90, ), (-0, ), (90, ), (180, )}



# @using_geometric
# @using_rdkit
# def test_service_gridoptimization_single_noopt(fractal_compute_server):
#
#     client = ptl.FractalClient(fractal_compute_server)
#
#     # Add a HOOH
#     hooh = ptl.data.get_molecule("hooh.json")
#     initial_distance = hooh.measure([1, 2])
#
#     # Options
#     service = GridOptimizationInput(**{
#         "keywords": {
#             "preoptimization": False,
#             "scans": [{
#                 "type": "distance",
#                 "indices": [1, 2],
#                 "steps": [-0.1, 0.0],
#                 "step_type": "relative"
#             }]
#         },
#         "optimization_spec": {
#             "program": "geometric",
#             "keywords": {
#                 "coordsys": "tric",
#             }
#         },
#         "qc_spec": {
#             "driver": "gradient",
#             "method": "UFF",
#             "basis": "",
#             "keywords": None,
#             "program": "rdkit",
#         },
#         "initial_molecule": hooh,
#     }) # yapf: disable
#
#     ret = client.add_service([service])
#     fractal_compute_server.await_services()
#     assert len(fractal_compute_server.list_current_tasks()) == 0
#
#     result = client.query_procedures(id=ret.ids)[0]
#
#     assert result.status == "COMPLETE"
#     assert result.starting_grid == (1, )
#     assert pytest.approx(result.get_final_energies((0, )), abs=1.e-4) == 0.00032145876568280524
#
#     assert result.starting_molecule == result.initial_molecule
#
#     # Check initial vs startin molecule
#     assert result.initial_molecule == result.starting_molecule
#
#     mol = client.query_molecules(id=result.starting_molecule)[0]
#     assert pytest.approx(mol.measure([1, 2])) == initial_distance
#
#
