"""
    Unit tests of the SQLAlchemt to PostgreSQL interface to MongoDB

    Does not use portal or fractal server interfaces.

"""

from time import time

import pytest
from sqlalchemy.orm import joinedload

import qcfractal.interface as ptl
from qcfractal.services.services import TorsionDriveService
from qcfractal.storage_sockets.models import (
    KVStoreORM,
    MoleculeORM,
    OptimizationHistory,
    OptimizationProcedureORM,
    ResultORM,
    ServiceQueueORM,
    TaskQueueORM,
    TorsionDriveProcedureORM,
    Trajectory,
)
from qcfractal.testing import sqlalchemy_socket_fixture as storage_socket


def session_delete_all(session, className):
    rows = session.query(className).all()
    for row in rows:
        session.delete(row)

    session.commit()
    return len(rows)


@pytest.fixture(scope="function")
def session(storage_socket):

    session = storage_socket.Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@pytest.fixture
def molecules_H4O2(storage_socket):
    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    ret = storage_socket.add_molecules([water, water2])

    yield list(ret["data"])

    r = storage_socket.del_molecules(molecule_hash=[water.get_hash(), water2.get_hash()])
    assert r == 2


@pytest.fixture
def kw_fixtures(storage_socket):
    kw1 = ptl.models.KeywordSet(**{"values": {"something": "kwfixture"}})
    ret = storage_socket.add_keywords([kw1])

    yield list(ret["data"])

    r = storage_socket.del_keywords(ret["data"][0])
    assert r == 1


@pytest.mark.parametrize("compression", ptl.models.CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
def test_kvstore(session, compression, compression_level):

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

    session_delete_all(session, KVStoreORM)


def test_old_kvstore(storage_socket, session):
    """
    Tests retrieving old data from KVStore
    TODO: Remove once entire migration is complete
    """

    assert session.query(KVStoreORM).count() == 0

    input_str = "This is some input " * 10

    # Manually create the ORM, setting only the 'value' member
    # (This replicates what an existing database would have)
    log = KVStoreORM(value=input_str)
    session.add(log)
    session.commit()

    # Now query through the interface
    q = storage_socket.get_kvstore([log.id])["data"][str(log.id)]
    assert q.data.decode() == input_str
    assert q.compression is ptl.models.CompressionEnum.none
    assert q.compression_level == 0

    session_delete_all(session, KVStoreORM)


def test_molecule_sql(storage_socket, session):
    """
    Test the use of the ME class MoleculeORM

    Note:
        creation of a MoleculeORM using ME is not implemented yet
        Should create a MoleculeORM using: mongoengine_socket.add_molecules
    """

    num_mol_in_db = session.query(MoleculeORM).count()
    # MoleculeORM.objects().delete()
    assert num_mol_in_db == 0

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    water2 = ptl.data.get_molecule("water_dimer_stretch.psimol")

    # Add MoleculeORM
    ret = storage_socket.add_molecules([water, water2])
    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_inserted"] == 2

    ret = storage_socket.get_molecules()

    assert ret["meta"]["n_found"] == 2

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

    # Clean up
    storage_socket.del_molecules(molecule_hash=[water.get_hash(), water2.get_hash()])


def test_services(storage_socket, session):

    assert session.query(OptimizationProcedureORM).count() == 0

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    ret = storage_socket.add_molecules([water])
    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_inserted"] == 1

    proc_data = {
        "initial_molecule": ret["data"][0],
        "keywords": None,
        "program": "p7",
        "qc_spec": {"basis": "b1", "program": "p1", "method": "m1", "driver": "energy"},
        "status": "COMPLETE",
        "protocols": {},
    }

    service_data = {
        "tag": "tag1 tag2",
        "hash_index": "123",
        "status": "COMPLETE",
        "optimization_program": "gaussian",
        # extra fields
        "torsiondrive_state": {},
        "dihedral_template": "1",  # Not realistic?
        "optimization_template": "2",  # Not realistic?
        "molecule_template": "",
        "logger": None,
        "storage_socket": storage_socket,
        "task_priority": 0,
    }

    procedure = OptimizationProcedureORM(**proc_data)
    session.add(procedure)
    session.commit()
    assert procedure.id

    service_pydantic = TorsionDriveService(**service_data)

    doc = ServiceQueueORM(**service_pydantic.dict(include=set(ServiceQueueORM.__dict__.keys())))
    doc.extra = service_pydantic.dict(exclude=set(ServiceQueueORM.__dict__.keys()))
    doc.procedure_id = procedure.id
    doc.priority = doc.priority.value  # Special case where we need the value not the enum
    session.add(doc)
    session.commit()

    session.delete(doc)
    session.delete(procedure)
    session.commit()

    assert session.query(ServiceQueueORM).count() == 0


def test_results_sql(storage_socket, session, molecules_H4O2, kw_fixtures):
    """
    Handling results throught the ME classes
    """

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
        "status": "COMPLETE",
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
        "status": "COMPLETE",
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

    # clean up
    session_delete_all(session, ResultORM)


def test_optimization_procedure(storage_socket, session, molecules_H4O2):
    """
    Optimization procedure
    """

    assert session.query(OptimizationProcedureORM).count() == 0
    # assert Keywords.objects().count() == 0

    data1 = {
        "procedure": "optimization",
        "initial_molecule": molecules_H4O2[0],
        "keywords": None,
        "program": "p7",
        "qc_spec": {"basis": "b1", "program": "p1", "method": "m1", "driver": "energy"},
        "status": "COMPLETE",
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
        "status": "COMPLETE",
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

    # clean up
    session_delete_all(session, ResultORM)
    session_delete_all(session, OptimizationProcedureORM)


def test_torsiondrive_procedure(storage_socket, session):
    """
    Torsiondrive procedure
    """

    assert session.query(TorsionDriveProcedureORM).count() == 0

    water = ptl.data.get_molecule("water_dimer_minima.psimol")
    ret = storage_socket.add_molecules([water])
    assert ret["meta"]["success"] is True
    assert ret["meta"]["n_inserted"] == 1

    data1 = {
        "keywords": None,
        "program": "p9",
        "qc_spec": {"basis": "b1", "program": "p1", "method": "m1", "driver": "energy"},
        "status": "COMPLETE",
        "protocols": {},
    }

    torj_proc = TorsionDriveProcedureORM(**data1)
    session.add(torj_proc)
    session.commit()

    # Add optimization_history

    data1["initial_molecule"] = ret["data"][0]
    opt_proc = OptimizationProcedureORM(**data1)
    opt_proc2 = OptimizationProcedureORM(**data1)
    session.add_all([opt_proc, opt_proc2])
    session.commit()
    assert opt_proc.id

    opt_hist = OptimizationHistory(torsion_id=torj_proc.id, opt_id=opt_proc.id, key="20")
    opt_hist2 = OptimizationHistory(torsion_id=torj_proc.id, opt_id=opt_proc2.id, key="20")
    torj_proc.optimization_history_obj = [opt_hist, opt_hist2]
    session.commit()
    torj_proc = session.query(TorsionDriveProcedureORM).options(joinedload("optimization_history_obj")).first()
    assert torj_proc.optimization_history == {"20": [str(opt_proc.id), str(opt_proc2.id)]}

    # clean up
    session_delete_all(session, OptimizationProcedureORM)
    # session_delete_all(session, TorsionDriveProcedureORM)


def test_add_task_queue(storage_socket, session, molecules_H4O2):
    """
    Simple test of adding a task using the SQL classes
    in QCFractal, tasks should be added using storage_socket
    """

    assert session.query(TaskQueueORM).count() == 0
    # TaskQueueORM.objects().delete()

    page1 = {
        "procedure": "single",
        "molecule": molecules_H4O2[0],
        "method": "m1",
        "basis": "b1",
        "keywords": None,
        "program": "p1",
        "driver": "energy",
        "protocols": {},
    }
    # add a task that reference results
    result = ResultORM(**page1)
    session.add(result)
    session.commit()

    task = TaskQueueORM(base_result_obj=result, spec={"something": True})
    session.add(task)
    session.commit()

    ret = session.query(TaskQueueORM)
    assert ret.count() == 1

    task = ret.first()
    assert task.status == "WAITING"
    assert task.base_result_obj.status == "INCOMPLETE"

    # cleanup
    session_delete_all(session, TaskQueueORM)
    session_delete_all(session, ResultORM)


def test_results_pagination(storage_socket, session, molecules_H4O2, kw_fixtures):
    """
    Test results pagination
    """

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

    # cleanup
    session_delete_all(session, ResultORM)
