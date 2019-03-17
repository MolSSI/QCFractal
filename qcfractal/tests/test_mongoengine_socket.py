"""
    Unit tests of the mongoengine interface to MongoDB

    Does not use portal or fractal server interfaces.

"""

from time import time

import pytest
from bson import ObjectId

import qcfractal.interface as ptl
from qcfractal.storage_sockets.me_models import (MoleculeORM, OptimizationProcedureORM, ProcedureORM, ResultORM,
                                                 TaskQueueORM, TorsiondriveProcedureORM)
from qcfractal.testing import mongoengine_socket_fixture as storage_socket


@pytest.fixture
def molecules_H4O2(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    ret = storage_socket.add_molecules([water, water2])

    yield list(ret['data'])

    r = storage_socket.del_molecules(molecule_hash=[water.get_hash(), water2.get_hash()])
    assert r == 2


@pytest.fixture
def kw_fixtures(storage_socket):
    kw1 = ptl.models.KeywordSet(**{"values": {"something": "kwfixture"}})
    ret = storage_socket.add_keywords([kw1])

    yield list(ret['data'])

    r = storage_socket.del_keywords(ret['data'][0])
    assert r == 1


def test_molecule(storage_socket):
    """
        Test the use of the ME class MoleculeORM

        Note:
            creation of a MoleculeORM using ME is not implemented yet
            Should create a MoleculeORM using: mongoengine_socket.add_molecules
    """

    # don't use len(MoleculeORM.objects), slow
    num_mol_in_db = MoleculeORM.objects().count()
    # MoleculeORM.objects().delete()
    assert num_mol_in_db == 0

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    # Add MoleculeORM using pymongo
    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_inserted"] == 2

    # Use the ORM class
    water_mol = MoleculeORM.objects().first()
    assert water_mol.molecular_formula == "H4O2"
    assert water_mol.molecular_charge == 0

    # print(water_mol.json_dict())

    # Query with fields in the model
    result_list = MoleculeORM.objects(molecular_formula="H4O2")
    assert len(result_list) == 2
    assert result_list[0].molecular_multiplicity == 1

    # Query with fields NOT in the model. works too!
    result_list = MoleculeORM.objects(molecular_charge=0)
    assert len(result_list) == 2

    # get unique by hash and formula
    one_mol = MoleculeORM.objects(molecule_hash=water_mol.molecule_hash, molecular_formula=water_mol.molecular_formula)
    assert len(one_mol) == 1

    # Clean up
    storage_socket.del_molecules(molecule_hash=[water.get_hash(), water2.get_hash()])


def test_results(storage_socket, molecules_H4O2, kw_fixtures):
    """
        Handling results throught the ME classes
    """

    assert ResultORM.objects().count() == 0

    assert len(molecules_H4O2) == 2

    page1 = {
        "molecule": molecules_H4O2[0],
        "method": "m1",
        "basis": "b1",
        "keywords": None,
        "program": "p1",
        "driver": "energy",
        "other_data": 5,
        "status": "COMPLETE",
    }

    page2 = {
        "molecule": ObjectId(molecules_H4O2[1]),
        "method": "m2",
        "basis": "b1",
        "keywords": kw_fixtures[0],
        "program": "p1",
        "driver": "energy",
        "other_data": 10,
        "status": "COMPLETE",
    }

    ResultORM(**page1).save()
    ret = ResultORM.objects(method='m1').first()
    assert ret.molecule.fetch().molecular_formula == 'H4O2'
    assert ret.keywords is None

    ResultORM(**page2).save()
    ret = ResultORM.objects(method='m2').first()
    assert ret.molecule.fetch().molecular_formula == 'H4O2'
    assert ret.method == "m2"

    # clean up
    ResultORM.objects().delete()


def test_procedure(storage_socket):
    """
        Handling procedure throught the ME classes
    """

    assert ProcedureORM.objects().count() == 0
    # assert Keywords.objects().count() == 0

    # molecules = MoleculeORM.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        "procedure": "undefined",
        "keywords": None,
        "program": "p5",
        "qc_meta": {
            "basis": "b1",
            "program": "p1",
            "method": "11",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique",
        "status": "COMPLETE",
    }

    procedure = ProcedureORM(**data1)
    procedure.save()
    assert procedure.id
    # print('ProcedureORM After save: ', procedure.json_dict())
    # assert procedure.molecule.molecular_formula == 'H4O2'


def test_optimization_procedure(storage_socket, molecules_H4O2):
    """
        Optimization procedure
    """

    assert OptimizationProcedureORM.objects().count() == 0
    # assert Keywords.objects().count() == 0

    data1 = {
        "initial_molecule": ObjectId(molecules_H4O2[0]),
        # "procedure_type": None,
        "keywords": None,
        "program": "p7",
        "qc_meta": {
            "basis": "b1",
            "program": "p1",
            "method": "m1",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique_opt1",
        "status": "COMPLETE",
    }

    procedure = OptimizationProcedureORM(**data1).save()
    proc = OptimizationProcedureORM.objects().first()
    assert proc.initial_molecule.fetch().molecular_formula == 'H4O2'


def test_torsiondrive_procedure(storage_socket):
    """
        Torsiondrive procedure
    """

    assert TorsiondriveProcedureORM.objects().count() == 0
    # assert Keywords.objects().count() == 0

    # molecules = MoleculeORM.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        # "procedure": None,
        "keywords": None,
        "program": "p9",
        "qc_meta": {
            "basis": "b1",
            "program": "p1",
            "method": "m1",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique_td1",
        "status": "COMPLETE",
    }

    procedure = TorsiondriveProcedureORM(**data1)
    procedure.save()
    # print('TorsiondriveProcedureORM After save: ', procedure.json_dict())


def test_add_task_queue(storage_socket, molecules_H4O2):
    """
        Simple test of adding a task using the ME classes
        in QCFractal, tasks should be added using storage_socket
    """

    assert TaskQueueORM.objects.count() == 0
    TaskQueueORM.objects().delete()

    page1 = {
        "molecule": ObjectId(molecules_H4O2[0]),
        "method": "m1",
        "basis": "b1",
        "keywords": None,
        "program": "p1",
        "driver": "energy",
        "other_data": 5,
        "status": "COMPLETE",
    }
    # add a task that reference results
    result = ResultORM(**page1).save()

    task = TaskQueueORM(base_result=result)
    task.save()
    assert TaskQueueORM.objects().count() == 1

    # add a task that reference Optimization ProcedureORM
    opt = OptimizationProcedureORM.objects().first()

    task = TaskQueueORM(base_result=opt)
    task.save()
    assert TaskQueueORM.objects().count() == 2

    # add a task that reference Torsiondrive ProcedureORM
    data1 = {
        "keywords": None,
        "program": "P9",
        "qc_meta": {
            "basis": "b1",
            "program": "p1",
            "method": "m1",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique_td2",
        "status": "COMPLETE",
    }

    tor = TorsiondriveProcedureORM(**data1).save()

    task = TaskQueueORM(base_result=tor)
    task.save()
    assert TaskQueueORM.objects().count() == 3

    # cleanup
    ResultORM.objects.delete()
    TaskQueueORM.objects.delete()


def test_results_pagination(storage_socket, molecules_H4O2, kw_fixtures):
    """
        Test results pagination
    """

    assert ResultORM.objects().count() == 0

    result_template = {
        "molecule": ObjectId(molecules_H4O2[0]),
        "method": "m1",
        "basis": "b1",
        "keywords": kw_fixtures[0],
        "program": "p1",
        "driver": "energy",
        "status": "COMPLETE",
    }

    # Save (~ 1 msec/doc)
    t1 = time()

    total_results = 1000
    first_half = int(total_results / 2)
    limit = 100
    skip = 50

    for i in range(first_half):
        result_template['basis'] = str(i)
        ResultORM(**result_template).save()

    result_template['method'] = 'm2'
    for i in range(first_half, total_results):
        result_template['basis'] = str(i)
        ResultORM(**result_template).save()

    # total_time = (time() - t1) * 1000 / total_results
    # print('Inserted {} results in {:.2f} msec / doc'.format(total_results, total_time))

    # query (~ 0.13 msec/doc)
    t1 = time()

    ret1 = ResultORM.objects(method='m1')
    ret2 = ResultORM.objects(method='m2').limit(limit).skip(skip)

    data1 = [d.to_json_obj() for d in ret1]
    data2 = [d.to_json_obj() for d in ret2]

    # count is total, but actual data size is the limit
    assert ret1.count() == first_half
    assert len(data1) == first_half

    assert ret2.count() == total_results - first_half
    assert len(ret2) == limit
    assert len(data2) == limit

    assert int(data2[0]['basis']) == first_half + skip

    # get the last page when with fewer than limit are remaining
    ret = ResultORM.objects(method='m1').limit(limit).skip(int(first_half - limit / 2))
    assert len(ret) == limit / 2

    # total_time = (time() - t1) * 1000 / total_results
    # print('Query {} results in {:.2f} msec /doc'.format(total_results, total_time))

    # cleanup
    ResultORM.objects.delete()


def test_queue(storage_socket):
    tasks = TaskQueueORM.objects(status='WAITING')\
                .limit(1000)\
                .order_by('-created_on')\
                .select_related()   # *** no lazy load of ReferenceField, get them now (trurns of dereferencing, max_depth=1)
    # .only(projections_list)
    # .fields(..)
    # .exculde(..)
    # .no_dereference()  # don't get any of the ReferenceFields (ids) (Turning off dereferencing)
    assert len(tasks) == 0
