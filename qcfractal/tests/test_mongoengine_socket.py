"""
    Unit tests of the mongoengine interface to MongoDB

    Does not use portal or fractal server interfaces.

"""

import pytest
import qcfractal.interface as portal

from qcfractal.storage_sockets.mongoengine_socket import MongoengineSocket
from qcfractal.storage_sockets.models import Molecule, Result, Procedure
import mongoengine as db


@pytest.fixture(scope='module')
def mongoengine_socket():
    # Drop DB if it exists
    db_client = db.connect('test_qc_mongoengine')
    db_client.drop_database('test_qc_mongoengine')

    # connect to the DB using MongoengineSocket
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

