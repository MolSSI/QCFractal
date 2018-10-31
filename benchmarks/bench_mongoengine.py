"""
This tests the Mongoenegine performance when handling Results and procedures

"""

import numpy as np
from time import time
import mongoengine as db
from qcfractal.storage_sockets.models import Molecule, Result, Options
from qcfractal.storage_sockets.mongoengine_socket import MongoengineSocket
import qcfractal.interface as portal
from time import time

db_client = db.connect('bench_qc_mongoengine')
db_client.drop_database('bench_qc_mongoengine')

mongoengine_socket = MongoengineSocket("mongodb://localhost", 'bench_qc_mongoengine')
# mongoengine_socket.mongoengine_client.drop_database('bech_qc_mongoengine')

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
        tmp['charge'] = i
        mol_data['water'+str(i)] = tmp

    mongoengine_socket.add_molecules(mol_data)


def insert_results(n_results):

    option = Options(program='Psi4').save()
    # repeat searching for the molecule
    for i in range(n_results):
        mol = Molecule.objects.first()  # one DB access
        # option = Options.objects().first()
        data = {
            "molecule": mol,
            "method": str(i),
            "basis": "B1",
            "options": None,
            "program": "P1",
            "driver": "energy",
            "other_data": 5,
        }
        Result(**data).save()  # second DB access


def bulk_insert_results(n_results):

    results = []
    for i in range(n_results):
        mol = Molecule.objects.first()  # one DB access
        # option = Options.objects().first()
        data = {
            "molecule": mol,
            "method": str(i),
            "basis": "B1",
            "options": None,
            "program": "P1",
            "driver": "energy",
            "other_data": 5,
        }
        results.append(Result(**data))
    Result.objects.insert(results)


def query_results(n_query):
    for i in range(n_query):
        # mol = Molecule.objects.first()  # --> the overhead in this query
        option = Options.objects(program='Psi4').first()  # or [0], throws ex
        query = {
            # "molecule": mol,
            "method": str(i),
            "basis": "B1",
            "options": option,
            "program": "P1",
            "driver": "energy",
        }
        Result.objects(**query)  # second DB access


tstart = time()
insert_molecules(n_mol)
dtime = (time() - tstart) * 1000  # msec
print('{} Molecules inserted in an avg {:0.3f} ms / doc'.format(n_mol, dtime/n_mol))


tstart = time()
insert_results(n_results)
dtime = (time() - tstart) * 1000  # msec
print('{} Results inserted in an avg {:0.3f} ms / doc'.format(n_results, dtime/n_results))

# ---> Bulk in much faster
tstart = time()
bulk_insert_results(n_results)
dtime = (time() - tstart) * 1000  # msec
print('{} Bulk Results inserted in an avg {:0.3f} ms / doc'.format(n_results, dtime/n_results))


tstart = time()
query_results(n_query)
dtime = (time() - tstart) * 1000  # msec
print('{} Results queries in an avg {:0.3f} ms / doc'.format(n_query, dtime/n_query))


