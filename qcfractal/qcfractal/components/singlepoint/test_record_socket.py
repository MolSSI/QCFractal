from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcarchivetesting import load_molecule_data
from qcfractal.components.testing_helpers import convert_to_plain_qcschema_result
from qcportal.managers import ManagerName
from qcportal.molecules import Molecule
from qcportal.record_models import RecordStatusEnum, PriorityEnum, RecordTask
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols
from qcportal.utils import now_at_utc
from .record_db_models import SinglepointRecordORM
from .testing_helpers import test_specs, load_test_data, run_test_data
from ..record_utils import build_extras_properties

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session
    from typing import Dict, List


def coalesce(x):
    if x is None:
        return ""
    return x


def _compare_record_with_schema(record_orm, result_schema):
    assert record_orm.status == RecordStatusEnum.complete
    assert record_orm.specification.program == result_schema.provenance.creator.lower()

    assert record_orm.specification.driver == result_schema.driver
    assert record_orm.specification.method == result_schema.model.method

    # some task specs still return NULL/None for basis
    assert coalesce(record_orm.specification.basis) == coalesce(result_schema.model.basis)

    assert record_orm.specification.keywords == result_schema.keywords
    assert record_orm.specification.protocols == result_schema.protocols

    assert len(record_orm.compute_history) == 1
    assert record_orm.compute_history[0].status == RecordStatusEnum.complete

    assert record_orm.compute_history[0].provenance == result_schema.provenance

    # Use plain schema, where compressed stuff is removed
    new_extras, new_properties = build_extras_properties(result_schema.copy(deep=True))
    assert record_orm.properties == new_properties
    assert record_orm.extras == new_extras

    # do some common properties themselves
    result_dict = result_schema.dict(include={"return_result"}, encoding="json")
    assert record_orm.properties.get("nuclear_repulsion_energy") == result_schema.properties.nuclear_repulsion_energy
    assert record_orm.properties.get("return_energy") == result_schema.properties.return_energy
    assert record_orm.properties.get("scf_iterations") == result_schema.properties.scf_iterations
    assert record_orm.properties.get("scf_total_energy") == result_schema.properties.scf_total_energy
    assert record_orm.properties.get("return_result") == result_dict["return_result"]

    if result_schema.wavefunction is not None:
        wfn_1 = record_orm.wavefunction.get_wavefunction().dict(encoding="json")
        wfn_2 = record_orm.wavefunction.get_wavefunction().dict(encoding="json")
        wfn_ref = result_schema.wavefunction.dict(encoding="json")
        assert wfn_1 == wfn_2 == wfn_ref
    else:
        assert record_orm.wavefunction is None

    if result_schema.native_files is not None:
        assert record_orm.native_files.keys() == result_schema.native_files.keys()

        for k, v in result_schema.native_files.items():
            assert record_orm.native_files[k].get_file() == v
    else:
        assert record_orm.native_files is None

    for k in ("stdout", "stderr", "error"):
        plain_output = getattr(result_schema, k)
        if plain_output is not None:
            out_str = record_orm.compute_history[0].outputs[k].get_output()
            assert out_str == plain_output
        else:
            assert k not in record_orm.compute_history[0].outputs


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
        assert t.compute_tag == "tag1"
        assert t.compute_priority == PriorityEnum.low

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

        plain_result = convert_to_plain_qcschema_result(result)
        _compare_record_with_schema(record, plain_result)

        # some extra stuff
        desc_info = storage_socket.records.get_short_descriptions([rec_id])[0]
        short_desc = desc_info["description"]
        assert desc_info["record_type"] == record.record_type
        assert desc_info["created_on"] == record.created_on
        assert record.specification.program in short_desc
        assert record.specification.method in short_desc


def test_singlepoint_socket_insert_complete_schema_v1(storage_socket: SQLAlchemySocket, session: Session):
    test_names = [
        "sp_psi4_benzene_energy_1",
        "sp_psi4_benzene_energy_2",
        "sp_psi4_benzene_energy_3",
        "sp_psi4_fluoroethane_wfn",
        "sp_psi4_h2_b3lyp_nativefiles",
        "sp_psi4_peroxide_energy_wfn",
        "sp_psi4_water_energy",
        "sp_psi4_water_gradient",
        "sp_psi4_water_hessian",
        "sp_rdkit_benzene_energy",
        "sp_rdkit_water_energy",
    ]

    all_ids = []

    for test_name in test_names:
        _, _, result_schema = load_test_data(test_name)

        plain_schema = convert_to_plain_qcschema_result(result_schema)

        # Need a full copy of results - they can get mutated
        with storage_socket.session_scope() as session2:
            ins_ids_1 = storage_socket.records.insert_complete_schema_v1(session2, [result_schema.copy(deep=True)])
            ins_ids_2 = storage_socket.records.insert_complete_schema_v1(session2, [plain_schema.copy(deep=True)])

        ins_id_1 = ins_ids_1[0]
        ins_id_2 = ins_ids_2[0]

        # insert_complete_schema always inserts
        assert ins_id_1 != ins_id_2
        assert ins_id_1 not in all_ids
        assert ins_id_2 not in all_ids
        all_ids.extend([ins_id_1, ins_id_2])

        rec_1 = session.get(SinglepointRecordORM, ins_id_1)
        rec_2 = session.get(SinglepointRecordORM, ins_id_2)

        _compare_record_with_schema(rec_1, plain_schema)
        _compare_record_with_schema(rec_2, plain_schema)
