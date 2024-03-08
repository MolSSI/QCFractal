from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcportal.compression import decompress
from qcportal.managers import ManagerName
from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum, PriorityEnum, RecordTask
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols
from qcportal.utils import now_at_utc
from .record_db_models import SinglepointRecordORM
from .testing_helpers import test_specs, load_test_data, run_test_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session
    from typing import Dict, List


def coalesce(x):
    if x is None:
        return ""
    return x


@pytest.mark.parametrize("spec", test_specs)
def test_singlepoint_socket_task_spec(
    storage_socket: SQLAlchemySocket,
    spec: QCSpecification,
    activated_manager_name: ManagerName,
    activated_manager_programs: Dict[str, List[str]],
):
    water = load_molecule_data("water_dimer_minima")
    hooh = load_molecule_data("hooh")
    ne4 = load_molecule_data("neon_tetramer")
    all_mols = [water, hooh, ne4]

    time_0 = now_at_utc()
    meta, id = storage_socket.records.singlepoint.add(all_mols, spec, "tag1", PriorityEnum.low, None, None, True)
    time_1 = now_at_utc()
    assert meta.success

    tasks = storage_socket.tasks.claim_tasks(activated_manager_name.fullname, activated_manager_programs, ["*"])
    tasks = [RecordTask(**t) for t in tasks]

    assert len(tasks) == 3
    for t in tasks:
        function_kwargs = t.function_kwargs
        assert function_kwargs["input_data"]["model"] == {"method": spec.method, "basis": spec.basis}
        assert function_kwargs["input_data"]["protocols"] == spec.protocols.dict(exclude_defaults=True)
        assert function_kwargs["input_data"]["keywords"] == spec.keywords
        assert function_kwargs["program"] == spec.program
        assert t.tag == "tag1"
        assert t.priority == PriorityEnum.low

    rec_id_mol_map = {
        id[0]: all_mols[0],
        id[1]: all_mols[1],
        id[2]: all_mols[2],
    }

    assert Molecule(**tasks[0].function_kwargs["input_data"]["molecule"]) == rec_id_mol_map[tasks[0].record_id]
    assert Molecule(**tasks[1].function_kwargs["input_data"]["molecule"]) == rec_id_mol_map[tasks[1].record_id]
    assert Molecule(**tasks[2].function_kwargs["input_data"]["molecule"]) == rec_id_mol_map[tasks[2].record_id]


def test_singlepoint_socket_find_existing_1(storage_socket: SQLAlchemySocket):
    spec = QCSpecification(
        program="prog1",
        driver=SinglepointDriver.energy,
        method="b3lyp",
        basis="6-31G*",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="all"),
    )

    water = load_molecule_data("water_dimer_minima")
    meta, id1 = storage_socket.records.singlepoint.add([water], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_find_existing_2(storage_socket: SQLAlchemySocket):
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
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_find_existing_3(storage_socket: SQLAlchemySocket):
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
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_find_existing_4(storage_socket: SQLAlchemySocket):
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
    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add([water], spec2, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_find_existing_5(storage_socket: SQLAlchemySocket):
    # Test adding molecule by id

    water = load_molecule_data("water_dimer_minima")
    kw = {"a": "value"}
    _, mol_ids = storage_socket.molecules.add([water])

    spec1 = QCSpecification(program="prog1", driver=SinglepointDriver.energy, method="b3lyp", basis=None, keywords=kw)

    spec2 = QCSpecification(program="prog1", driver=SinglepointDriver.energy, method="b3lyp", basis="", keywords=kw)

    meta, id1 = storage_socket.records.singlepoint.add([water], spec1, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.singlepoint.add(mol_ids, spec2, "*", PriorityEnum.normal, None, None, True)
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


def test_singlepoint_socket_run(
    storage_socket: SQLAlchemySocket, session: Session, activated_manager_name: ManagerName
):
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

    for rec_id, result in zip(all_id, all_results):
        record = session.get(SinglepointRecordORM, rec_id)
        assert record.status == RecordStatusEnum.complete
        assert record.specification.program == result.provenance.creator.lower()
        assert record.specification.driver == result.driver
        assert record.specification.method == result.model.method

        # some task specs still return NULL/None for basis
        assert coalesce(record.specification.basis) == coalesce(result.model.basis)

        assert record.specification.keywords == result.keywords
        assert record.specification.protocols == result.protocols

        assert len(record.compute_history) == 1
        assert record.compute_history[0].status == RecordStatusEnum.complete
        assert record.compute_history[0].provenance == result.provenance

        desc_info = storage_socket.records.get_short_descriptions([rec_id])[0]
        short_desc = desc_info["description"]
        assert desc_info["record_type"] == record.record_type
        assert desc_info["created_on"] == record.created_on
        assert record.specification.program in short_desc
        assert record.specification.method in short_desc

        # Compressed outputs should have been removed
        assert "_qcfractal_compressed_outputs" not in record.extras
        assert "_qcfractal_compressed_native_files" not in record.extras

        result_dict = result.dict(include={"return_result"}, encoding="json")
        assert record.properties.get("nuclear_repulsion_energy") == result.properties.nuclear_repulsion_energy
        assert record.properties.get("return_energy") == result.properties.return_energy
        assert record.properties.get("scf_iterations") == result.properties.scf_iterations
        assert record.properties.get("scf_total_energy") == result.properties.scf_total_energy
        assert record.properties.get("return_result") == result_dict["return_result"]

        if record.wavefunction is None:
            assert result.wavefunction is None
        else:
            assert result.wavefunction is not None
            wfn_prop = record.wavefunction.get_wavefunction()
            assert wfn_prop.dict(encoding="json") == result.wavefunction.dict(encoding="json")

        if not record.native_files:
            assert not result.native_files
        else:
            avail_nf = set(record.native_files.keys())
            result_nf = set(result.native_files.keys()) if result.native_files is not None else set()
            compressed_nf = result.extras.get("_qcfractal_compressed_native_files", {})
            result_nf |= set(compressed_nf.keys())
            assert avail_nf == result_nf

        outs = record.compute_history[0].outputs

        avail_outputs = set(outs.keys())
        result_outputs = {x for x in ["stdout", "stderr", "error"] if getattr(result, x, None) is not None}
        compressed_outputs = result.extras.get("_qcfractal_compressed_outputs", {})
        result_outputs |= set(compressed_outputs.keys())
        assert avail_outputs == result_outputs

        # NOTE - this only works for string outputs (not dicts)
        # but those are used for errors, which aren't covered here
        for out in outs.values():
            o_str = out.get_output()
            co = result.extras["_qcfractal_compressed_outputs"][out.output_type]
            ro = decompress(co["data"], co["compression_type"])
            assert o_str == ro


def test_singlepoint_socket_insert(storage_socket: SQLAlchemySocket, session: Session):
    input_spec_2, molecule_2, result_data_2 = load_test_data("sp_psi4_peroxide_energy_wfn")

    # Need a full copy of results - they can get mutated
    result_copy = result_data_2.copy(deep=True)

    meta2, id2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "*", PriorityEnum.normal, None, None, True
    )

    # Typical workflow
    rec_orm = session.get(SinglepointRecordORM, id2[0])
    storage_socket.records.update_completed_task(session, rec_orm, result_data_2, None)

    # Actually insert the whole thing
    with storage_socket.session_scope() as session2:
        dup_id = storage_socket.records.insert_complete_record(session2, [result_copy])

    recs = [session.get(SinglepointRecordORM, id2[0]), session.get(SinglepointRecordORM, dup_id)]

    assert recs[0].id != recs[1].id
    assert recs[0].status == RecordStatusEnum.complete
    assert recs[1].status == RecordStatusEnum.complete

    assert recs[0].specification == recs[1].specification

    assert len(recs[0].compute_history) == 1
    assert len(recs[1].compute_history) == 1
    assert recs[0].compute_history[0].status == RecordStatusEnum.complete
    assert recs[1].compute_history[0].status == RecordStatusEnum.complete

    assert recs[0].compute_history[0].provenance == recs[1].compute_history[0].provenance

    assert recs[0].properties == recs[1].properties

    wfn_model_1 = recs[0].wavefunction.get_wavefunction()
    wfn_model_2 = recs[1].wavefunction.get_wavefunction()
    assert wfn_model_1.dict(encoding="json") == wfn_model_2.dict(encoding="json")

    assert len(recs[0].compute_history[0].outputs) == 1
    assert len(recs[1].compute_history[0].outputs) == 1

    out_str_1 = recs[0].compute_history[0].outputs["stdout"].get_output()
    out_str_2 = recs[1].compute_history[0].outputs["stdout"].get_output()
    assert out_str_1 == out_str_2
