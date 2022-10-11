from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest
from qcelemental.models.results import AtomicResultProperties

from qcarchivetesting import load_molecule_data
from qcfractal.components.wavefunctions.test_db_models import assert_wfn_equal
from qcportal.compression import decompress_string
from qcportal.managers import ManagerName
from qcportal.molecules import Molecule
from qcportal.outputstore import OutputStore
from qcportal.record_models import RecordStatusEnum, PriorityEnum
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols
from qcportal.wavefunctions.models import WavefunctionProperties
from .record_db_models import SinglepointRecordORM
from .testing_helpers import test_specs, load_test_data, run_test_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


@pytest.mark.parametrize("spec", test_specs)
def test_singlepoint_socket_task_spec(
    storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName, spec: QCSpecification
):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.singlepoint.add(all_mols, spec, "tag1", PriorityEnum.low, None, None)
    time_1 = datetime.utcnow()
    assert meta.success

    tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname)

    assert len(tasks) == 3
    for t in tasks:
        assert t["spec"]["args"][0]["model"] == {"method": spec.method, "basis": spec.basis}
        assert t["spec"]["args"][0]["protocols"] == spec.protocols.dict(exclude_defaults=True)
        assert t["spec"]["args"][0]["keywords"] == spec.keywords
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


def test_singlepoint_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_2(storage_socket: SQLAlchemySocket):
    # Test case sensitivity
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    spec2 = QCSpecification(
        program="pRog1",
        driver=SinglepointDriver.energy,
        method="b3lYp",
        basis="6-31g*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_3(storage_socket: SQLAlchemySocket):
    # Test default keywords and protocols
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={},
        protocols=SinglepointProtocols(wavefunction="none"),
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_4(storage_socket: SQLAlchemySocket):
    # Test None basis
    spec1 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis=None,
    )

    spec2 = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="",
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_add_same_5(storage_socket: SQLAlchemySocket):
    # Test adding molecule by id

    water = load_molecule_data("water_dimer_minima")
    kw = {"a": "value"}
    _, mol_ids = storage_socket.molecules.add([water])

    spec1 = QCSpecification(program="prog1", driver=SinglepointDriver.energy, method="b3lyp", basis=None, keywords=kw)

    spec2 = QCSpecification(program="prog1", driver=SinglepointDriver.energy, method="b3lyp", basis="", keywords=kw)

    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add(mol_ids, spec2, "*", PriorityEnum.normal, None, None)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_run(storage_socket: SQLAlchemySocket, activated_manager_name: ManagerName):

    test_names = [
        "sp_psi4_benzene_energy_1",
        "sp_psi4_peroxide_energy_wfn",
        "sp_rdkit_water_energy",
        "sp_psi4_h2_b3lyp_nativefiles",
    ]

    all_results = []
    all_id = []

    for test_name in test_names:
        _, _, result_data = load_test_data(test_name)
        record_id = run_test_data(storage_socket, activated_manager_name, test_name)
        all_results.append(result_data)
        all_id.append(record_id)

    recs = storage_socket.records.singlepoint.get(
        all_id,
        include=["*", "wavefunction", "compute_history.*", "compute_history.outputs", "native_files"],
    )

    for record, result in zip(recs, all_results):
        assert record["status"] == RecordStatusEnum.complete
        assert record["specification"]["program"] == result.provenance.creator.lower()
        assert record["specification"]["driver"] == result.driver
        assert record["specification"]["method"] == result.model.method
        assert record["specification"]["basis"] == result.model.basis
        assert record["specification"]["keywords"] == result.keywords
        assert record["specification"]["protocols"] == result.protocols

        assert len(record["compute_history"]) == 1
        assert record["compute_history"][0]["status"] == RecordStatusEnum.complete
        assert record["compute_history"][0]["provenance"] == result.provenance

        # Compressed outputs should have been removed
        assert "_qcfractal_compressed_outputs" not in record["extras"]
        assert "_qcfractal_compressed_native_files" not in record["extras"]

        # assert record["return_result"] == result.return_result
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

        nf = record.get("native_files", None)
        if not nf:
            assert not result.native_files
        else:
            avail_nf = set(record["native_files"].keys())
            result_nf = set(result.native_files.keys()) if result.native_files is not None else set()
            compressed_nf = result.extras.get("_qcfractal_compressed_native_files", {})
            result_nf |= set(compressed_nf.keys())
            assert avail_nf == result_nf

        outs = record["compute_history"][0]["outputs"]

        avail_outputs = set(outs.keys())
        result_outputs = {x for x in ["stdout", "stderr", "error"] if getattr(result, x, None) is not None}
        compressed_outputs = result.extras.get("_qcfractal_compressed_outputs", [])
        result_outputs |= set(x["output_type"] for x in compressed_outputs)
        assert avail_outputs == result_outputs

        # NOTE - this only works for string outputs (not dicts)
        # but those are used for errors, which aren't covered here
        for o in outs.values():
            out_obj = OutputStore(**o)
            ro = getattr(result, o["output_type"], None)
            if ro is None:
                co = result.extras["_qcfractal_compressed_outputs"][0]
                ro = decompress_string(co["data"], co["compression"])
            assert out_obj.as_string == ro


def test_singlepoint_socket_insert(storage_socket: SQLAlchemySocket):
    input_spec_2, molecule_2, result_data_2 = load_test_data("sp_psi4_peroxide_energy_wfn")

    # Need a full copy of results - they can get mutated
    result_copy = result_data_2.copy(deep=True)

    meta2, id2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "*", PriorityEnum.normal, None, None
    )

    # Typical workflow
    with storage_socket.session_scope() as session:
        rec_orm = session.query(SinglepointRecordORM).where(SinglepointRecordORM.id == id2[0]).one()
        storage_socket.records.update_completed_task(session, rec_orm, result_data_2, None)

    # Actually insert the whole thing
    with storage_socket.session_scope() as session:
        dup_id = storage_socket.records.insert_complete_record(session, [result_copy])

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
    outs1 = OutputStore(**recs[0]["compute_history"][0]["outputs"]["stdout"])
    outs2 = OutputStore(**recs[1]["compute_history"][0]["outputs"]["stdout"])
    assert outs1.as_string == outs2.as_string
