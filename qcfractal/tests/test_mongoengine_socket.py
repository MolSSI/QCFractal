"""
    Unit tests of the mongoengine interface to MongoDB

    Does not use portal or fractal server interfaces.

"""

import qcfractal.interface as portal

from qcfractal.storage_sockets.models import Molecule, Result, Options, \
                    Procedure, OptimizationProcedure, TorsiondriveProcedure
from qcfractal.storage_sockets.models import TaskQueue
from qcfractal.testing import mongoengine_socket_fixture as storage_socket


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
    ret = storage_socket.add_molecules({"water1": water.json(as_dict=True),
                                            "water2": water2.json(as_dict=True)})
    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_inserted"] == 2

    # Use the ORM class
    water_mol = Molecule.objects().first()
    assert water_mol.molecular_formula == "H4O2"
    assert water_mol.molecular_charge == 0

    # print(water_mol.json(as_dict=True))

    # Query with fields in the model
    result_list = Molecule.objects(molecular_formula="H4O2")
    assert len(result_list) == 2
    assert result_list[0].molecular_multiplicity == 1

    # Query with fields NOT in the model. works too!
    result_list = Molecule.objects(molecular_charge=0)
    assert len(result_list) == 2

    # get unique by hash and formula
    one_mol = Molecule.objects(molecule_hash=water_mol.molecule_hash,
                               molecular_formula=water_mol.molecular_formula)
    assert len(one_mol) == 1


def test_results(storage_socket):
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
    # print('Result After save: ', result.json(as_dict=True))
    assert result.molecule.molecular_formula == 'H4O2'


def test_procedure(storage_socket):
    """
        Handling procedure throught the ME classes
    """

    assert Procedure.objects().count() == 0
    # assert Options.objects().count() == 0

    # molecules = Molecule.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        "procedure": "undefined",
        "options": None,
        "program": "P5",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
    }

    procedure = Procedure(**data1)
    procedure.save()
    # print('Procedure After save: ', procedure.json(as_dict=True))
    # assert procedure.molecule.molecular_formula == 'H4O2'


def test_optimization_procedure(storage_socket):
    """
        Optimization procedure
    """

    assert OptimizationProcedure.objects().count() == 0
    # assert Options.objects().count() == 0

    molecules = Molecule.objects(molecular_formula='H4O2')

    data1 = {
        "initial_molecule": molecules[0],
        # "procedure_type": None,
        "options": None,
        "program": "P7",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
    }

    procedure = OptimizationProcedure(**data1)
    procedure.save()
    # print('OptimizationProcedure After save: ', procedure.json(as_dict=True))
    assert procedure.initial_molecule.molecular_formula == 'H4O2'


def test_torsiondrive_procedure(storage_socket):
    """
        Torsiondrive procedure
    """

    assert TorsiondriveProcedure.objects().count() == 0
    # assert Options.objects().count() == 0

    # molecules = Molecule.objects(molecular_formula='H4O2')
    # assert molecules.count() == 2

    data1 = {
        # "molecule": molecules[0],
        # "procedure": None,
        "options": None,
        "program": "P9",
        "qc_meta": {
            "basis": "B1",
            "program": "P1",
            "method": "M1",
            "driver": "energy"
        },
    }

    procedure = TorsiondriveProcedure(**data1)
    procedure.save()
    # print('TorsiondriveProcedure After save: ', procedure.json(as_dict=True))


def test_add_task_queue(storage_socket):
    """
        Simple test of adding a task using the ME classes
        in QCFractal, tasks should be added using storage_socket
    """

    assert TaskQueue.objects.count() == 0
    TaskQueue.objects().delete()

    # add a task that reference results
    result = Result.objects().first()

    task = TaskQueue(base_result=result)
    task.save()
    assert TaskQueue.objects().count() == 1

    # add a task that reference Optimization Procedure
    opt = OptimizationProcedure.objects().first()

    task = TaskQueue(base_result=opt)
    task.save()
    assert TaskQueue.objects().count() == 2

    # add a task that reference Torsiondrive Procedure
    tor = TorsiondriveProcedure.objects().first()

    task = TaskQueue(base_result=tor)
    task.save()
    assert TaskQueue.objects().count() == 3


def test_queue(storage_socket):
    tasks = TaskQueue.objects(status='WAITING')\
                .limit(1000)\
                .order_by('-created_on')\
                .select_related()   # *** no lazy load of ReferenceField, get them now (trurns of dereferencing, max_depth=1)
                # .only(projections_list)
                # .fields(..)
                # .exculde(..)
                # .no_dereference()  # don't get any of the ReferenceFields (ids) (Turning off dereferencing)
    assert len(tasks) == 3