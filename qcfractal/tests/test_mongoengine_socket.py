"""
    Unit tests of the mongoengine interface to MongoDB

    Does not use portal or fractal server interfaces.

"""

from time import time

import pytest
import qcfractal.interface as portal
from bson import ObjectId
from qcfractal.storage_sockets.me_models import (Molecule, OptimizationProcedure, Procedure, Result,
                                                 TaskQueue, TorsiondriveProcedure)
from qcfractal.testing import mongoengine_socket_fixture as storage_socket


@pytest.fixture
def molecules_H4O2(storage_socket):
    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")

    ret = storage_socket.add_molecules([water, water2.json_dict()])

    yield list(ret['data'])

    r = storage_socket.del_molecules(molecule_hash=[water.get_hash(), water2.get_hash()])
    assert r == 2


@pytest.fixture
def kw_fixtures(storage_socket):
    kw1 = portal.models.KeywordSet(**{"values": {"something": "kwfixture"}})
    ret = storage_socket.add_keywords([kw1.dict()])

    yield list(ret['data'])

    r = storage_socket.del_keywords(ret['data'][0])
    assert r == 1


def test_molecule(storage_socket):
    """
        Test the use of the ME class Molecule

        Note:
            creation of a Molecule using ME is not implemented yet
            Should create a Molecule using: mongoengine_socket.add_molecules
    """

    # don't use len(Molecule.objects), slow
    num_mol_in_db = Molecule.objects().count()
    # Molecule.objects().delete()
    assert num_mol_in_db == 0

    water = portal.data.get_molecule("water_dimer_minima.psimol")
    water2 = portal.data.get_molecule("water_dimer_stretch.psimol")

    # Add Molecule using pymongo
    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_inserted"] == 2

    # Use the ORM class
    water_mol = Molecule.objects().first()
    assert water_mol.molecular_formula == "H4O2"
    assert water_mol.molecular_charge == 0

    # print(water_mol.json_dict())

    # Query with fields in the model
    result_list = Molecule.objects(molecular_formula="H4O2")
    assert len(result_list) == 2
    assert result_list[0].molecular_multiplicity == 1

    # Query with fields NOT in the model. works too!
    result_list = Molecule.objects(molecular_charge=0)
    assert len(result_list) == 2

    # get unique by hash and formula
    one_mol = Molecule.objects(molecule_hash=water_mol.molecule_hash, molecular_formula=water_mol.molecular_formula)
    assert len(one_mol) == 1

    # Clean up
    storage_socket.del_molecules(molecule_hash=[water.get_hash(), water2.get_hash()])


def test_results(storage_socket, molecules_H4O2, kw_fixtures):
    """
        Handling results throught the ME classes
    """

    assert Result.objects().count() == 0

    assert len(molecules_H4O2) == 2

    page1 = {
        "molecule": molecules_H4O2[0],
        "method": "M1",
        "basis": "B1",
        "keywords": None,
        "program": "P1",
        "driver": "energy",
        "other_data": 5,
    }

    page2 = {
        "molecule": ObjectId(molecules_H4O2[1]),
        "method": "M2",
        "basis": "B1",
        "keywords": kw_fixtures[0],
        "program": "P1",
        "driver": "energy",
        "other_data": 10,
    }

    Result(**page1).save()
    ret = Result.objects(method='m1').first()
    assert ret.molecule.fetch().molecular_formula == 'H4O2'
    assert ret.keywords is None

    Result(**page2).save()
    ret = Result.objects(method='m2').first()
    assert ret.molecule.fetch().molecular_formula == 'H4O2'
    assert ret.method == "m2"

    # clean up
    Result.objects().delete()


def test_procedure(storage_socket):
    """
        Handling procedure throught the ME classes
    """

    assert Procedure.objects().count() == 0
    # assert Keywords.objects().count() == 0

    # molecules = Molecule.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        "procedure": "undefined",
        "keywords": None,
        "program": "P5",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique"
    }

    procedure = Procedure(**data1)
    procedure.save()
    assert procedure.id
    # print('Procedure After save: ', procedure.json_dict())
    # assert procedure.molecule.molecular_formula == 'H4O2'


def test_optimization_procedure(storage_socket, molecules_H4O2):
    """
        Optimization procedure
    """

    assert OptimizationProcedure.objects().count() == 0
    # assert Keywords.objects().count() == 0

    data1 = {
        "initial_molecule": ObjectId(molecules_H4O2[0]),
        # "procedure_type": None,
        "keywords": None,
        "program": "P7",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique_opt1"
    }

    procedure = OptimizationProcedure(**data1).save()
    proc = OptimizationProcedure.objects().first()
    assert proc.initial_molecule.fetch().molecular_formula == 'H4O2'


def test_torsiondrive_procedure(storage_socket):
    """
        Torsiondrive procedure
    """

    assert TorsiondriveProcedure.objects().count() == 0
    # assert Keywords.objects().count() == 0

    # molecules = Molecule.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        # "procedure": None,
        "keywords": None,
        "program": "P9",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique_td1"
    }

    procedure = TorsiondriveProcedure(**data1)
    procedure.save()
    # print('TorsiondriveProcedure After save: ', procedure.json_dict())


def test_add_task_queue(storage_socket, molecules_H4O2):
    """
        Simple test of adding a task using the ME classes
        in QCFractal, tasks should be added using storage_socket
    """

    assert TaskQueue.objects.count() == 0
    TaskQueue.objects().delete()

    page1 = {
        "molecule": ObjectId(molecules_H4O2[0]),
        "method": "M1",
        "basis": "B1",
        "keywords": None,
        "program": "P1",
        "driver": "energy",
        "other_data": 5,
    }
    # add a task that reference results
    result = Result(**page1).save()

    task = TaskQueue(base_result=result)
    task.save()
    assert TaskQueue.objects().count() == 1

    # add a task that reference Optimization Procedure
    opt = OptimizationProcedure.objects().first()

    task = TaskQueue(base_result=opt)
    task.save()
    assert TaskQueue.objects().count() == 2

    # add a task that reference Torsiondrive Procedure
    data1 = {
        "keywords": None,
        "program": "P9",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
        "hash_index": "somethingveryunique_td2"
    }

    tor = TorsiondriveProcedure(**data1).save()

    task = TaskQueue(base_result=tor)
    task.save()
    assert TaskQueue.objects().count() == 3

    # cleanup
    Result.objects.delete()
    TaskQueue.objects.delete()


def test_results_pagination(storage_socket, molecules_H4O2, kw_fixtures):
    """
        Test results pagination
    """

    assert Result.objects().count() == 0

    result_template = {
        "molecule": ObjectId(molecules_H4O2[0]),
        "method": "M1",
        "basis": "B1",
        "keywords": kw_fixtures[0],
        "program": "P1",
        "driver": "energy",
    }

    # Save (~ 1 msec/doc)
    t1 = time()

    total_results = 1000
    first_half = int(total_results / 2)
    limit = 100
    skip = 50

    for i in range(first_half):
        result_template['basis'] = str(i)
        Result(**result_template).save()

    result_template['method'] = 'm2'
    for i in range(first_half, total_results):
        result_template['basis'] = str(i)
        Result(**result_template).save()

    # total_time = (time() - t1) * 1000 / total_results
    # print('Inserted {} results in {:.2f} msec / doc'.format(total_results, total_time))

    # query (~ 0.13 msec/doc)
    t1 = time()

    ret1 = Result.objects(method='m1')
    ret2 = Result.objects(method='m2').limit(limit).skip(skip)

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
    ret = Result.objects(method='m1').limit(limit).skip(int(first_half - limit / 2))
    assert len(ret) == limit / 2

    # total_time = (time() - t1) * 1000 / total_results
    # print('Query {} results in {:.2f} msec /doc'.format(total_results, total_time))

    # cleanup
    Result.objects.delete()


def test_queue(storage_socket):
    tasks = TaskQueue.objects(status='WAITING')\
                .limit(1000)\
                .order_by('-created_on')\
                .select_related()   # *** no lazy load of ReferenceField, get them now (trurns of dereferencing, max_depth=1)
    # .only(projections_list)
    # .fields(..)
    # .exculde(..)
    # .no_dereference()  # don't get any of the ReferenceFields (ids) (Turning off dereferencing)
    assert len(tasks) == 0
