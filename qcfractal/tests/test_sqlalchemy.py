"""
    Unit tests of the SQLAlchemt to PostgreSQL interface to MongoDB

    Does not use portal or fractal server interfaces.

"""

from time import time

import pytest
from sqlalchemy.orm import joinedload

import qcfractal.interface as ptl
from qcfractal.interface.models import RecordStatusEnum
from qcfractal.storage_sockets.models import (
    ServiceQueueORM,
    TaskQueueORM,
)
from qcfractal.components.records.torsiondrive.db_models import OptimizationHistory, TorsionDriveProcedureORM
from qcfractal.components.records.optimization.db_models import Trajectory, OptimizationProcedureORM
from qcfractal.components.records.singlepoint.db_models import ResultORM
from qcfractal.components.outputstore.db_models import KVStoreORM
from qcfractal.components.molecule.db_models import MoleculeORM


@pytest.fixture(scope="function")
def session_fixture(storage_socket):
    with storage_socket.session_scope() as session:
        yield storage_socket, session


@pytest.fixture
def molecules_H4O2(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    meta, ret = storage_socket.molecule.add([water, water2])

    yield ret


@pytest.fixture
def kw_fixtures(storage_socket):
    kw1 = ptl.models.KeywordSet(**{"values": {"something": "kwfixture"}})
    _, ret = storage_socket.keywords.add([kw1])
    yield list(ret)


@pytest.mark.parametrize("compression", ptl.models.CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_kvstore(session_fixture, compression, compression_level):

    _, session = session_fixture
    assert session.query(KVStoreORM).count() == 0

    input_str = "This is some input " * 10
    kv = ptl.models.KVStore.compress(input_str, compression, compression_level)
    log = KVStoreORM(**kv.dict())
    session.add(log)
    session.commit()

    q = session.query(KVStoreORM).one()

    # TODO - remove the exclude once all data is migrated in DB
    # (there will be no "value" in the ORM anymore
    kv2 = ptl.models.KVStore(**q.to_dict(exclude=["value"]))
    assert kv2.get_string() == input_str
    assert kv2.compression is compression


def test_old_kvstore(session_fixture):
    """
    Tests retrieving old data from KVStore
    TODO: Remove once entire migration is complete
    """

    storage_socket, session = session_fixture
    assert session.query(KVStoreORM).count() == 0

    input_str = "This is some input " * 10

    # Manually create the ORM, setting only the 'value' member
    # (This replicates what an existing database would have)
    log = KVStoreORM(value=input_str)
    session.add(log)
    session.commit()

    # Now query through the interface
    q_dict = storage_socket.output_store.get([log.id])[0]
    q = ptl.models.KVStore(**q_dict)
    assert q.data.decode() == input_str
    assert q.compression is ptl.models.CompressionEnum.none
    assert q.compression_level == 0


def test_molecule_sql(session_fixture):
    """
    Test the use of the ME class MoleculeORM

    Note:
        creation of a MoleculeORM using ME is not implemented yet
        Should create a MoleculeORM using: mongoengine_socket.add_molecules
    """

    storage_socket, session = session_fixture
    num_mol_in_db = session.query(MoleculeORM).count()
    # MoleculeORM.objects().delete()
    assert num_mol_in_db == 0

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    # Add MoleculeORM
    meta, ret = storage_socket.molecule.add([water, water2])
    assert meta.success
    assert meta.n_inserted == 2

    meta, ret = storage_socket.molecule.query()
    assert meta.n_returned == 2

    # Use the ORM class
    water_mol = session.query(MoleculeORM).first()
    assert water_mol.molecular_formula == "H4O2"
    assert water_mol.molecular_charge == 0

    # print(water_mol.dict())
    #
    # Query with fields in the model
    result_list = session.query(MoleculeORM).filter_by(molecular_formula="H4O2").all()
    assert len(result_list) == 2
    assert result_list[0].molecular_multiplicity == 1

    # Query with fields NOT in the model. works too!
    result_list = session.query(MoleculeORM).filter_by(molecular_charge=0).all()
    assert len(result_list) == 2

    # get unique by hash and formula
    one_mol = session.query(MoleculeORM).filter_by(
        molecule_hash=water_mol.molecule_hash, molecular_formula=water_mol.molecular_formula
    )
    assert len(one_mol.all()) == 1


def test_results_sql(session_fixture, molecules_H4O2, kw_fixtures):
    """
    Handling results throught the ME classes
    """

    storage_socket, session = session_fixture
    assert session.query(ResultORM).count() == 0

    assert len(molecules_H4O2) == 2
    assert len(kw_fixtures) == 1

    page1 = {
        "procedure": "single",
        "molecule": molecules_H4O2[0],
        "method": "m1",
        "basis": "b1",
        "keywords": None,
        "program": "p1",
        "driver": "energy",
        "status": RecordStatusEnum.complete,
        "protocols": {},
    }

    page2 = {
        "procedure": "single",
        "molecule": molecules_H4O2[1],
        "method": "m2",
        "basis": "b1",
        "keywords": kw_fixtures[0],
        "program": "p1",
        "driver": "energy",
        "status": RecordStatusEnum.complete,
        "protocols": {},
    }

    result = ResultORM(**page1)
    session.add(result)
    session.commit()

    # IMPORTANT: To be able to access lazy loading children use joinedload
    ret = session.query(ResultORM).options(joinedload("molecule_obj")).filter_by(method="m1").first()
    assert ret.molecule_obj.molecular_formula == "H4O2"
    # Accessing the keywords_obj will issue a DB access
    assert ret.keywords_obj == None

    result2 = ResultORM(**page2)
    session.add(result2)
    session.commit()
    ret = session.query(ResultORM).options(joinedload("molecule_obj")).filter_by(method="m2").first()
    assert ret.molecule_obj.molecular_formula == "H4O2"
    assert ret.method == "m2"


def test_optimization_procedure(session_fixture, molecules_H4O2):
    """
    Optimization procedure
    """

    _, session = session_fixture
    assert session.query(OptimizationProcedureORM).count() == 0
    # assert Keywords.objects().count() == 0

    data1 = {
        "procedure": "optimization",
        "initial_molecule": molecules_H4O2[0],
        "keywords": None,
        "program": "p7",
        "qc_spec": {"basis": "b1", "program": "p1", "method": "m1", "driver": "energy"},
        "status": RecordStatusEnum.complete,
        "protocols": {},
    }

    result1 = {
        "procedure": "single",
        "molecule": molecules_H4O2[0],
        "method": "m1",
        "basis": "b1",
        "keywords": None,
        "program": "p1",
        "driver": "energy",
        "status": RecordStatusEnum.complete,
        "protocols": {},
    }

    procedure = OptimizationProcedureORM(**data1)
    session.add(procedure)
    session.commit()
    proc = session.query(OptimizationProcedureORM).options(joinedload("initial_molecule_obj")).first()
    assert proc.initial_molecule_obj.molecular_formula == "H4O2"
    assert proc.procedure == "optimization"

    # add a trajectory result
    result = ResultORM(**result1)
    session.add(result)
    session.commit()
    assert result.id

    # link result to the trajectory
    proc.trajectory_obj = [Trajectory(opt_id=proc.id, result_id=result.id)]
    session.commit()
    proc = session.query(OptimizationProcedureORM).options(joinedload("trajectory_obj")).first()
    assert proc.trajectory_obj


# def test_torsiondrive_procedure(session_fixture):
#    """
#    Torsiondrive procedure
#    """
#
#    storage_socket, session = session_fixture
#    assert session.query(TorsionDriveProcedureORM).count() == 0
#
#    water = ptl.data.get_molecule("water_dimer_minima.psimol")
#    meta, ret = storage_socket.molecule.add([water])
#    assert meta.success
#    assert meta.n_inserted == 1
#
#    data1 = {
#        "keywords": None,
#        "program": "p9",
#        "qc_spec": {"basis": "b1", "program": "p1", "method": "m1", "driver": "energy"},
#        "status": RecordStatusEnum.complete,
#        "protocols": {},
#    }
#
#    torj_proc = TorsionDriveProcedureORM(**data1)
#    session.add(torj_proc)
#    session.commit()
#
#    # Add optimization_history
#
#    data1["initial_molecule"] = ret[0]
#    opt_proc = OptimizationProcedureORM(**data1)
#    opt_proc2 = OptimizationProcedureORM(**data1)
#    session.add_all([opt_proc, opt_proc2])
#    session.commit()
#    assert opt_proc.id
#
#    opt_hist = OptimizationHistory(torsion_id=torj_proc.id, opt_id=opt_proc.id, key="20")
#    opt_hist2 = OptimizationHistory(torsion_id=torj_proc.id, opt_id=opt_proc2.id, key="20")
#    torj_proc.optimization_history_obj = [opt_hist, opt_hist2]
#    session.commit()
#    torj_proc = session.query(TorsionDriveProcedureORM).options(joinedload("optimization_history_obj")).first()
#    assert torj_proc.optimization_history == {"20": [str(opt_proc.id), str(opt_proc2.id)]}


def test_results_pagination(session_fixture, molecules_H4O2, kw_fixtures):
    """
    Test results pagination
    """

    _, session = session_fixture
    assert session.query(ResultORM).count() == 0

    result_template = {
        "procedure": "single",
        "molecule": molecules_H4O2[0],
        "method": "m1",
        "basis": "b1",
        "keywords": kw_fixtures[0],
        "program": "p1",
        "driver": "energy",
        "protocols": {},
    }

    # Save ~ 1 msec/doc in ME, 0.5 msec/doc in SQL
    # ------------------------------------------
    t1 = time()

    total_results = 1000
    first_half = int(total_results / 2)
    limit = 100
    skip = 50

    for i in range(first_half):
        result_template["basis"] = str(i)
        r = ResultORM(**result_template)
        session.add(r)

    result_template["method"] = "m2"
    for i in range(first_half, total_results):
        result_template["basis"] = str(i)
        r = ResultORM(**result_template)
        session.add(r)

    session.commit()  # must commit outside the loop, 10 times faster

    total_time = (time() - t1) * 1000 / total_results
    print("Inserted {} results in {:.2f} msec / doc".format(total_results, total_time))

    # query (~ 0.13 msec/doc) in ME, and ~0.02 msec/doc in SQL
    # ----------------------------------------
    t1 = time()

    ret1 = session.query(ResultORM).filter_by(method="m1")
    ret2 = session.query(ResultORM).filter_by(method="m2").limit(limit)  # .offset(skip)

    data1 = [d.to_dict() for d in ret1]
    data2 = [d.to_dict() for d in ret2]

    # count is total, but actual data size is the limit
    assert ret1.count() == first_half
    assert len(data1) == first_half

    # assert ret2.count() == total_results - first_half
    # assert len(ret2) == limit
    # assert len(data2) == limit
    #
    # assert int(data2[0]['basis']) == first_half + skip
    #
    # # get the last page when with fewer than limit are remaining
    # ret = session.query(ResultORM).filter_by(method='m1').limit(limit).offset(int(first_half - limit / 2))
    # assert len(ret) == limit / 2

    total_time = (time() - t1) * 1000 / total_results
    print("Query {} results in {:.3f} msec /doc".format(total_results, total_time))
