"""
    Unit tests of the mongoengine interface to MongoDB

    Does not use portal or fractal server interfaces.

"""

import pytest
import mongoengine as db
import qcfractal.interface as portal

from qcfractal.storage_sockets.mongoengine_socket import MongoengineSocket
from qcfractal.storage_sockets.models import Molecule, Result, Options, \
                    Procedure, OptimizationProcedure, TorsiondriveProcedure
from qcfractal.storage_sockets.models import TaskQueue


@pytest.fixture(scope='module')
def mongoengine_socket():
    # Drop DB if it exists
    db_client = db.connect('test_qc_mongoengine')
    db_client.drop_database('test_qc_mongoengine')

    # connect to the DB using the MongoengineSocket class
    mongoengine_socket = MongoengineSocket("mongodb://localhost", 'test_qc_mongoengine')

    yield mongoengine_socket

    # clean up, close connection, and delete Database
    mongoengine_socket.mongoengine_client.close()
    # mongoengine_socket.mongoengine_client.drop_database('test_qc_mongoengine')


def test_molecule(mongoengine_socket):
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
    ret = mongoengine_socket.add_molecules({"water1": water.to_json(),
                                            "water2": water2.to_json()})
    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_inserted"] == 2

    # Use the ORM class
    water_mol = Molecule.objects().first()
    assert water_mol.molecular_formula == "H4O2"
    assert water_mol.charge == 0

    # print(water_mol.to_json())

    # Query with fields in the model
    result_list = Molecule.objects(molecular_formula="H4O2")
    assert len(result_list) == 2
    assert result_list[0].multiplicity == 1

    # Query with fields NOT in the model. works too!
    result_list = Molecule.objects(charge=0)
    assert len(result_list) == 2

    # get unique by hash and formula
    one_mol = Molecule.objects(molecule_hash=water_mol.molecule_hash,
                               molecular_formula=water_mol.molecular_formula)
    assert len(one_mol) == 1


def test_results(mongoengine_socket):
    """
        Handling results throught the ME classes
    """

    assert Result.objects().count() == 0
    assert Options.objects().count() == 0

    molecules = Molecule.objects(molecular_formula='H4O2')

    assert molecules.count() == 2

    page1 = {
        "molecule": molecules[0],
        "method": "M1",
        "basis": "B1",
        "options": None,
        "program": "P1",
        "driver": "energy",
        "other_data": 5,
    }

    page2 = {
        "molecule": molecules[1],
        "method": "M1",
        "basis": "B1",
        "options": None,
        "program": "P1",
        "driver": "energy",
        "other_data": 10,
    }

    result = Result(**page1)
    result.save()
    # print('Result After save: ', result.to_json())
    assert result.molecule.molecular_formula == 'H4O2'


def test_procedure(mongoengine_socket):
    """
        Handling procedure throught the ME classes
    """

    assert Procedure.objects().count() == 0
    # assert Options.objects().count() == 0

    # molecules = Molecule.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        "procedure_type": "undefined",
        "procedure_options": None,
        "procedure_program": "P5",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
    }

    procedure = Procedure(**data1)
    procedure.save()
    # print('Procedure After save: ', procedure.to_json())
    # assert procedure.molecule.molecular_formula == 'H4O2'


def test_optimization_procedure(mongoengine_socket):
    """
        Optimization procedure
    """

    assert OptimizationProcedure.objects().count() == 0
    # assert Options.objects().count() == 0

    molecules = Molecule.objects(molecular_formula='H4O2')

    data1 = {
        "initial_molecule": molecules[0],
        # "procedure_type": None,
        "procedure_options": None,
        "procedure_program": "P7",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
    }

    procedure = OptimizationProcedure(**data1)
    procedure.save()
    # print('OptimizationProcedure After save: ', procedure.to_json())
    assert procedure.initial_molecule.molecular_formula == 'H4O2'


def test_torsiondrive_procedure(mongoengine_socket):
    """
        Torsiondrive procedure
    """

    assert TorsiondriveProcedure.objects().count() == 0
    # assert Options.objects().count() == 0

    # molecules = Molecule.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        # "procedure_type": None,
        "procedure_options": None,
        "procedure_program": "P9",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
    }

    procedure = TorsiondriveProcedure(**data1)
    procedure.save()
    # print('TorsiondriveProcedure After save: ', procedure.to_json())


def test_add_task_queue():
    """
        Simple test of adding a task using the ME classes
        in QCFractal, tasks should be added using mongoengine_socket
    """

    assert TaskQueue.objects.count() == 0
    TaskQueue.objects().delete()

    # add a task that reference results
    result = Result.objects().first()

    task = TaskQueue(baseResult=result)
    task.save()
    assert TaskQueue.objects().count() == 1

    # add a task that reference Optimization Procedure
    opt = OptimizationProcedure.objects().first()

    task = TaskQueue(baseResult=opt)
    task.save()
    assert TaskQueue.objects().count() == 2

    # add a task that reference Torsiondrive Procedure
    tor = TorsiondriveProcedure.objects().first()

    task = TaskQueue(baseResult=tor)
    task.save()
    assert TaskQueue.objects().count() == 3


@pytest.mark.skip
def test_queue():
    tasks = TaskQueue.objects(status='WAITING')\
                .limit(5)\
                .order_by('-created_on')\
                .select_related()   # *** no lazy load of ReferenceField, get them now (trurns of dereferencing, max_depth=1)
                # .only(projections_list)
                # .fields(..)
                # .exculde(..)
                # .no_dereference()  # don't get any of the ReferenceFields (ids) (Turning off dereferencing)
