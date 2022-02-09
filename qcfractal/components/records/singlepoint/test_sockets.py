"""
Tests the singlepoint record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest
from qcelemental.models.results import AtomicResultProperties

from qcfractal.components.records.singlepoint.db_models import SinglepointRecordORM
from qcfractal.components.wavefunctions.test_db_models import assert_wfn_equal
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.keywords import KeywordSet
from qcportal.managers import ManagerName
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.singlepoint import (
    QCSpecification,
    QCInputSpecification,
    SinglepointDriver,
    SinglepointProtocols,
    SinglepointQueryBody,
)
from qcportal.wavefunctions.models import WavefunctionProperties

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from typing import Dict, Any, Union


def compare_singlepoint_specs(
    input_spec: Union[QCInputSpecification, Dict[str, Any]],
    full_spec: Union[QCSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = QCInputSpecification(**input_spec)
    if isinstance(full_spec, QCSpecification):
        full_spec = full_spec.dict()

    full_spec.pop("id")
    full_spec.pop("keywords_id")
    full_spec["keywords"].pop("id")
    trimmed_spec = QCInputSpecification(**full_spec)
    return input_spec == trimmed_spec


_test_specs = [
    QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    ),
    QCInputSpecification(
        program="Prog2",
        driver=SinglepointDriver.gradient,
        method="Hf",
        basis="def2-TZVP",
        keywords=KeywordSet(values={"k": "v"}),
    ),
    QCInputSpecification(
        program="Prog3",
        driver=SinglepointDriver.hessian,
        method="pbe0",
        basis="",
        keywords=KeywordSet(values={"o": 1, "v": 2.123}),
        protocols=SinglepointProtocols(stdout=False, wavefunction="orbitals_and_eigenvalues"),
    ),
    QCInputSpecification(
        program="ProG4",
        driver=SinglepointDriver.hessian,
        method="pbe",
        basis=None,
        protocols=SinglepointProtocols(stdout=False, wavefunction="return_results"),
    ),
]


@pytest.mark.parametrize("spec", _test_specs)
def test_singlepoint_socket_add_get(storage_socket: SQLAlchemySocket, spec: QCInputSpecification):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.singlepoint.add(all_mols, spec, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.singlepoint.get(id, include=["*", "task", "molecule"])

    assert len(recs) == 3
    for r in recs:
        assert r["record_type"] == "singlepoint"
        assert compare_singlepoint_specs(spec, r["specification"])

        assert r["task"]["spec"] is None
        assert r["task"]["tag"] == "tag1"
        assert r["task"]["priority"] == PriorityEnum.low
        assert spec.program in r["task"]["required_programs"]

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["task"]["created_on"] < time_1

    mol1 = storage_socket.molecules.get([recs[0]["molecule_id"]])[0]
    mol2 = storage_socket.molecules.get([recs[1]["molecule_id"]])[0]
    mol3 = storage_socket.molecules.get([recs[2]["molecule_id"]])[0]
    assert mol1["identifiers"]["molecule_hash"] == water.get_hash()
    assert recs[0]["molecule"]["identifiers"]["molecule_hash"] == water.get_hash()

    assert mol2["identifiers"]["molecule_hash"] == hooh.get_hash()
    assert recs[1]["molecule"]["identifiers"]["molecule_hash"] == hooh.get_hash()

    assert mol3["identifiers"]["molecule_hash"] == ne4.get_hash()
    assert recs[2]["molecule"]["identifiers"]["molecule_hash"] == ne4.get_hash()


@pytest.mark.parametrize("spec", _test_specs)
def test_singlepoint_socket_task_spec(storage_socket: SQLAlchemySocket, spec: QCInputSpecification):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.singlepoint.add(all_mols, spec, tag="tag1", priority=PriorityEnum.low)
    time_1 = datetime.utcnow()
    assert meta.success

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"prog1": None, "prog2": "v3.0", "prog3": None, "prog4": None},
        tags=["*"],
    )
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    assert len(tasks) == 3
    for t in tasks:
        assert t["spec"]["args"][0]["model"] == {"method": spec.method, "basis": spec.basis}
        assert t["spec"]["args"][0]["protocols"] == spec.protocols.dict(exclude_defaults=True)
        assert t["spec"]["args"][0]["keywords"] == spec.keywords.values
        assert t["spec"]["args"][1] == spec.program
        assert t["tag"] == "tag1"
        assert t["priority"] == PriorityEnum.low
        assert time_0 < t["created_on"] < time_1

    rec_id_mol_map = {
        id[0]: all_mols[0],
        id[1]: all_mols[1],
        id[2]: all_mols[2],
    }

    assert Molecule(**tasks[0]["spec"]["args"][0]["molecule"]) == rec_id_mol_map[tasks[0]["record_id"]]
    assert Molecule(**tasks[1]["spec"]["args"][0]["molecule"]) == rec_id_mol_map[tasks[1]["record_id"]]
    assert Molecule(**tasks[2]["spec"]["args"][0]["molecule"]) == rec_id_mol_map[tasks[2]["record_id"]]


def test_singlepoint_socket_add_existing_molecule(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([ne4])

    # Now add records
    meta, id = storage_socket.records.singlepoint.add(all_mols, spec, tag="*", priority=PriorityEnum.normal)
    recs = storage_socket.records.singlepoint.get(id)

    assert len(recs) == 3
    assert recs[2]["molecule_id"] == mol_ids[0]


def test_singlepoint_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_2(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec1 = QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec2 = QCInputSpecification(
        program="pRog1",
        driver=SinglepointDriver.energy,
        method="b3lYp",
        basis="6-31g*",
        keywords=KeywordSet(values={"k": "value"}),
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_3(storage_socket: SQLAlchemySocket):
    # Test default keywords and protocols
    spec1 = QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords=KeywordSet(values={}),
        protocols=SinglepointProtocols(wavefunction="none"),
    )

    spec2 = QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_4(storage_socket: SQLAlchemySocket):
    # Test None basis
    spec1 = QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
    )

    spec2 = QCInputSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="",
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_5(storage_socket: SQLAlchemySocket):
    # Test adding keywords and molecule by id

    water = load_molecule_data("water_dimer_minima")
    kw = KeywordSet(values={"a": "value"})
    _, kw_ids = storage_socket.keywords.add([kw])
    _, mol_ids = storage_socket.molecules.add([water])

    spec1 = QCInputSpecification(
        program="prog1", driver=SinglepointDriver.energy, method="b3lyp", basis=None, keywords=kw
    )

    spec2 = QCInputSpecification(
        program="prog1", driver=SinglepointDriver.energy, method="b3lyp", basis="", keywords=kw_ids[0]
    )

    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add(mol_ids, spec2, tag="*", priority=PriorityEnum.normal)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_run(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("rdkit_water_energy")

    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta2, id2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    meta3, id3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    )

    result_map = {id1[0]: result_data_1, id2[0]: result_data_2, id3[0]: result_data_3}

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={
            "rdkit": None,
            "psi4": None,
        },
        tags=["*"],
    )

    tasks = storage_socket.tasks.claim_tasks(mname1.fullname, limit=100)
    assert len(tasks) == 3

    time_0 = datetime.utcnow()
    rmeta = storage_socket.tasks.update_finished(
        mname1.fullname,
        {
            tasks[0]["id"]: result_map[tasks[0]["record_id"]],
            tasks[1]["id"]: result_map[tasks[1]["record_id"]],
            tasks[2]["id"]: result_map[tasks[2]["record_id"]],
        },
    )
    time_1 = datetime.utcnow()
    assert rmeta.n_accepted == 3

    all_results = [result_data_1, result_data_2, result_data_3]
    recs = storage_socket.records.singlepoint.get(
        id1 + id2 + id3, include=["*", "wavefunction", "compute_history.*", "compute_history.outputs"]
    )

    for record, result in zip(recs, all_results):
        assert record["status"] == RecordStatusEnum.complete
        assert record["specification"]["program"] == result.provenance.creator.lower()
        assert record["specification"]["driver"] == result.driver
        assert record["specification"]["method"] == result.model.method
        assert record["specification"]["basis"] == result.model.basis
        assert record["specification"]["keywords"]["values"] == result.keywords
        assert record["specification"]["protocols"] == result.protocols
        assert record["created_on"] < time_0
        assert time_0 < record["modified_on"] < time_1

        assert len(record["compute_history"]) == 1
        assert record["compute_history"][0]["status"] == RecordStatusEnum.complete
        assert time_0 < record["compute_history"][0]["modified_on"] < time_1
        assert record["compute_history"][0]["provenance"] == result.provenance

        assert record["return_result"] == result.return_result
        arprop = AtomicResultProperties(**record["properties"])
        assert arprop.nuclear_repulsion_energy == result.properties.nuclear_repulsion_energy
        assert arprop.return_energy == result.properties.return_energy
        assert arprop.scf_iterations == result.properties.scf_iterations
        assert arprop.scf_total_energy == result.properties.scf_total_energy

        wfn = record.get("wavefunction", None)
        if wfn is None:
            assert result.wavefunction is None
        else:
            wfn_model = WavefunctionProperties(**record["wavefunction"])
            assert_wfn_equal(wfn_model, result.wavefunction)

        outs = record["compute_history"][0]["outputs"]

        avail_outputs = {x["output_type"] for x in outs}
        result_outputs = {x for x in ["stdout", "stderr", "error"] if getattr(result, x, None) is not None}
        assert avail_outputs == result_outputs

        # NOTE - this only works for string outputs (not dicts)
        # but those are used for errors, which aren't covered here
        for o in outs:
            out_obj = OutputStore(**o)
            ro = getattr(result, o["output_type"])
            assert out_obj.as_string == ro


def test_singlepoint_socket_insert(storage_socket: SQLAlchemySocket):
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")

    meta2, id2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )

    # Typical workflow
    with storage_socket.session_scope() as session:
        rec_orm = session.query(SinglepointRecordORM).where(SinglepointRecordORM.id == id2[0]).one()
        storage_socket.records.update_completed_task(session, rec_orm, result_data_2, None)

    # Actually insert the whole thing. This should end up being a duplicate
    with storage_socket.session_scope() as session:
        dup_id = storage_socket.records.insert_complete_record(session, [result_data_2])

    recs = storage_socket.records.singlepoint.get(
        id2 + dup_id, include=["*", "wavefunction", "compute_history.*", "compute_history.outputs"]
    )

    assert recs[0]["id"] != recs[1]["id"]
    assert recs[0]["status"] == RecordStatusEnum.complete
    assert recs[1]["status"] == RecordStatusEnum.complete

    assert recs[0]["specification"] == recs[1]["specification"]

    assert len(recs[0]["compute_history"]) == 1
    assert len(recs[1]["compute_history"]) == 1
    assert recs[0]["compute_history"][0]["status"] == RecordStatusEnum.complete
    assert recs[1]["compute_history"][0]["status"] == RecordStatusEnum.complete

    assert recs[0]["compute_history"][0]["provenance"] == recs[1]["compute_history"][0]["provenance"]

    assert recs[0]["return_result"] == recs[1]["return_result"]
    arprop1 = AtomicResultProperties(**recs[0]["properties"])
    arprop2 = AtomicResultProperties(**recs[1]["properties"])
    assert arprop1.nuclear_repulsion_energy == arprop2.nuclear_repulsion_energy
    assert arprop1.return_energy == arprop2.return_energy
    assert arprop1.scf_iterations == arprop2.scf_iterations
    assert arprop1.scf_total_energy == arprop2.scf_total_energy

    wfn_model_1 = WavefunctionProperties(**recs[0]["wavefunction"])
    wfn_model_2 = WavefunctionProperties(**recs[1]["wavefunction"])
    assert_wfn_equal(wfn_model_1, wfn_model_2)

    assert len(recs[0]["compute_history"][0]["outputs"]) == 1
    assert len(recs[1]["compute_history"][0]["outputs"]) == 1
    outs1 = OutputStore(**recs[0]["compute_history"][0]["outputs"][0])
    outs2 = OutputStore(**recs[1]["compute_history"][0]["outputs"][0])
    assert outs1.as_string == outs2.as_string


def test_singlepoint_socket_query(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("rdkit_water_energy")

    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )
    meta2, id2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    )
    meta3, id3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    )

    recs = storage_socket.records.singlepoint.get(id1 + id2 + id3)

    # query for molecule
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(molecule_id=[recs[1]["molecule_id"]]))
    assert meta.n_found == 1
    assert sp[0]["id"] == id2[0]

    # query for program
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(program=["psi4"]))
    assert meta.n_found == 2
    assert {sp[0]["id"], sp[1]["id"]} == set(id1 + id2)

    # query for basis
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(basis=["sTO-3g"]))
    assert meta.n_found == 1
    assert sp[0]["id"] == id2[0]

    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(basis=[None]))
    assert meta.n_found == 1
    assert sp[0]["id"] == id3[0]

    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(basis=[""]))
    assert meta.n_found == 1
    assert sp[0]["id"] == id3[0]

    # query for method
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(method=["b3lyP"]))
    assert meta.n_found == 2

    # keyword id
    meta, sp = storage_socket.records.singlepoint.query(
        SinglepointQueryBody(keywords_id=[recs[0]["specification"]["keywords_id"]])
    )
    assert meta.n_found == 3  # All have empty keywords

    # driver
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(driver=[SinglepointDriver.energy]))
    assert meta.n_found == 3

    # Some empty queries
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(driver=[SinglepointDriver.properties]))
    assert meta.n_found == 0

    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(basis=["madeupbasis"]))
    assert meta.n_found == 0

    # Query by default returns everything
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody())
    assert meta.n_found == 3

    # Query by default (with a limit)
    meta, sp = storage_socket.records.singlepoint.query(SinglepointQueryBody(limit=1))
    assert meta.n_found == 3
    assert meta.n_returned == 1


def test_singlepoint_socket_delete_1(storage_socket: SQLAlchemySocket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_peroxide_energy_wfn")
    meta1, id1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, tag="*", priority=PriorityEnum.normal
    )

    with storage_socket.session_scope() as session:
        rec_orm = session.query(SinglepointRecordORM).where(SinglepointRecordORM.id == id1[0]).one()
        storage_socket.records.update_completed_task(session, rec_orm, result_data_1, None)

    # deleting with children is ok (even though we don't have children)
    meta = storage_socket.records.delete(id1, soft_delete=True, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]

    meta = storage_socket.records.delete(id1, soft_delete=False, delete_children=True)
    assert meta.success
    assert meta.deleted_idx == [0]

    recs = storage_socket.records.get(id1, missing_ok=True)
    assert recs == [None]
