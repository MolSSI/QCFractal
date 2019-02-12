"""
This tests the Mongoenegine performance when handling Results and procedures

"""

import mongoengine as db
from qcfractal.storage_sockets.models import Molecule, Result, Keywords
from qcfractal.storage_sockets.mongoengine_socket import MongoengineSocket
import qcfractal.interface as portal
from time import time
import pymongo
from pymongo import MongoClient


db_name = 'bench_qc_mongoengine'
db_client = db.connect(db_name)
db_client.drop_database(db_name)

mongoengine_socket = MongoengineSocket("mongodb://localhost", db_name)
# mongoengine_socket.mongoengine_client.drop_database('bech_qc_mongoengine')

pymongo_client = MongoClient()[db_name]

n_mol = 1000
n_results = 1000
n_query = 1000


def insert_molecules(n_mol):
    # add Molecules
    water = portal.data.get_molecule("water_dimer_minima.psimol").to_json()

    mol_data = {}
    # Add Molecule using pymongo
    for i in range(n_mol):
        tmp = water.copy()
        tmp['molecular_charge'] = i
        mol_data['water'+str(i)] = tmp

    mongoengine_socket.add_molecules(mol_data)


def query_molecule(n_mol):
    for i in range(n_mol):
        Molecule.objects.first()

def insert_results(n_results, mol):

    # repeat searching for the molecule
    for i in range(n_results):
        # mol = Molecule.objects.first()  # one DB access
        # option = Keywords.objects().first()
        data = {
            "molecule": mol.id,
            "method": str(i),
            "basis": "B1",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
            "other_data": 5,
        }
        results = Result(**data).save()  # second DB access
    return results


def bulk_insert_results(n_results, mol):

    results = []
    for i in range(n_results):
        # mol = Molecule.objects.first()  # one DB access
        # option = Keywords.objects().first()
        data = {
            "molecule": mol,
            "method": str(i),
            "basis": "Bulk",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
            "other_data": 5,
        }
        results.append(Result(**data))
    return Result.objects.insert(results)


def bulk_insert_results_pymongo(n_results, mol):

    results = []
    for i in range(n_results):
        # mol = Molecule.objects.first()  # one DB access
        # option = Keywords.objects().first()
        data = {
            "molecule": mol.id,
            "method": str(i),
            "basis": "Bulk pymongo",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
            "other_data": 5,
        }
        results.append(data)

    # results = Result._get_collection().insert_many(results, ordered=False)
    ret = pymongo_client.results.insert_many(results, ordered=False)

    return ret


def duplicate_results(n_results, mol):
    """Half the documents are duplicates"""

    tosave_results = []
    for i in range(n_results):
        # mol = Molecule.objects.first()  # one DB access
        # option = Keywords.objects().first()
        data = {
            "molecule": mol,
            "method": str(i + int(n_results/2)),
            "basis": "Bulk",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
            "other_data": 5,
        }
        found = Result.objects(**data).first()
        if not found:
            tosave_results.append(Result(**data))

    Result.objects.insert(tosave_results)
    print('Duplciates: ', len(tosave_results))


def duplicate_results_pymongo(n_results, mol):
    """Half the documents are duplicates"""

    results = []
    for i in range(n_results):
        # mol = Molecule.objects.first()  # one DB access
        # option = Keywords.objects().first()
        data = {
            "molecule": mol.id,
            "method": str(i + int(n_results/2)),
            "basis": "Bulk pymongo",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
            "other_data": 5,
        }
        results.append(data)

    try:
        # results = Result._get_collection().insert_many(results, ordered=False)
        pymongo_client.results.insert_many(results, ordered=False)
    except pymongo.errors.BulkWriteError as err:
        print('number of inserted: ', err.details["nInserted"])

    return results


def query_results(n_query, mol):
    for i in range(n_query):
        # mol = Molecule.objects.first()  # --> the overhead in this query
        # option = Keywords.objects(program='Psi4').first()  # or [0], throws ex
        query = {
            "molecule": mol,
            "method": str(i),
            "basis": "B1",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
        }
        Result.objects(**query)  # second DB access


def query_results_pymongo(n_query, mol):
    for i in range(n_query):
        # mol = Molecule.objects.first()  # --> the overhead in this query
        # option = Keywords.objects(program='Psi4').first()  # or [0], throws ex
        query = {
            "molecule": mol.id,
            "method": str(i),
            "basis": "B1",
            "keywords": None,
            "program": "P1",
            "driver": "energy",
        }
        # Result._get_collection().find_one(query)
        # db.connection.get_connection()[db_name]['result'].find_one(query)
        pymongo_client.results.find(query)

def bench():

    option = Keywords(program='Psi4', name='default').save()

    tstart = time()
    insert_molecules(n_mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Molecules inserted in an avg {:0.3f} ms / doc'.format(n_mol, dtime/n_mol))

    tstart = time()
    query_molecule(n_mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Query Molecules in an avg {:0.3f} ms / doc'.format(n_mol, dtime/n_mol))

    print('---------------------------')

    mol = Molecule.objects.first()

    tstart = time()
    insert_results(n_results, mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Results inserted in an avg {:0.3f} ms / doc'.format(n_results, dtime/n_results))

    # ---> Bulk in much faster
    tstart = time()
    bulk_insert_results(n_results, mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Bulk Results inserted in an avg {:0.3f} ms / doc'.format(n_results, dtime/n_results))

     # ---> Bulk in much faster
    tstart = time()
    bulk_insert_results_pymongo(n_results, mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Bulk Results inserted PYMONGO in an avg {:0.3f} ms / doc'.format(n_results, dtime/n_results))


    print('--------------------------')

    # ---> Duplicates
    tstart = time()
    duplicate_results(n_results, mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Duplicate Results inserted in an avg {:0.3f} ms / doc'.format(n_results, dtime/n_results))

    # ---> Duplicates
    tstart = time()
    duplicate_results_pymongo(n_results, mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Duplicate Results PYMONGO inserted in an avg {:0.3f} ms / doc'.format(n_results, dtime/n_results))

    print('--------------------------')

    tstart = time()
    query_results(n_query, mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Results queries in an avg {:0.3f} ms / doc'.format(n_query, dtime/n_query))

    tstart = time()
    query_results_pymongo(n_query, mol)
    dtime = (time() - tstart) * 1000  # msec
    print('{} Results queries PYMONGO in an avg {:0.3f} ms / doc'.format(n_query, dtime/n_query))


if __name__ == "__main__":
    bench()

