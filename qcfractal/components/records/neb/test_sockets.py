"""
Tests the neb record socket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.testing_helpers import run_service_constropt
from qcfractaltesting import load_molecule_data, load_procedure_data
from qcportal.outputstore import OutputStore
from qcportal.records import RecordStatusEnum, PriorityEnum
from qcportal.records.singlepoint import (
    QCSpecification,
    SinglepointProtocols,
)
from qcportal.records.neb import (
    NEBSpecification,
    NEBKeywords,
    NEBQueryBody,
)

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from typing import Dict, Any, Union


def compare_neb_specs(
    input_spec: Union[NEBSpecification, Dict[str, Any]],
    output_spec: Union[NEBSpecification, Dict[str, Any]],
) -> bool:
    if isinstance(input_spec, dict):
        input_spec = NEBSpecification(**input_spec)
    if isinstance(output_spec, dict):
        output_spec = NEBSpecification(**output_spec)

    return input_spec == output_spec


_test_specs = [
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
            program="psi4",
            keywords={"k": "value"},
            driver="gradient",
            method="b3lyp",
            basis="6-31g",
            protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
    NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=0.5,
            energy_weighted=True,
        ),
        qc_specification=QCSpecification(
            program="psi4",
            keywords={"k": "value"},
            driver="gradient",
            method="CCSD(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all"),
            ),
        ),
]



@pytest.mark.parametrize("spec", _test_specs)
def test_neb_socket_add_get(storage_socket: SQLAlchemySocket, spec: NEBSpecification):
    chain1 = [load_molecule_data("neb/neb_NCH_%i" %i) for i in range(43)]
    chain2 = [load_molecule_data("neb/neb_C3H2N_%i" %i) for i in range(60)]

    time_0 = datetime.utcnow()
    meta, id = storage_socket.records.neb.add(
        [chain1, chain2], spec, tag="tag1", priority=PriorityEnum.low
    )
    time_1 = datetime.utcnow()
    assert meta.success

    recs = storage_socket.records.neb.get(id, include=["*", "initial_chain", "initial_chain.molecule", "service"])

    assert len(recs) == 2
    for r in recs:
        assert r["record_type"] == "neb"
        assert r["status"] == RecordStatusEnum.waiting
        assert compare_neb_specs(spec, r["specification"])

        # Service queue entry should exist with the proper tag and priority
        assert r["service"]["tag"] == "tag1"
        assert r["service"]["priority"] == PriorityEnum.low

        assert time_0 < r["created_on"] < time_1
        assert time_0 < r["modified_on"] < time_1
        assert time_0 < r["service"]["created_on"] < time_1

    assert len(recs[0]["initial_chain"]) == len(chain1)
    assert len(recs[1]["initial_chain"]) == len(chain2)

    assert recs[0]["initial_chain"][0]['molecule']["identifiers"]["molecule_hash"] == chain1[0].get_hash()
    assert recs[0]["initial_chain"][6]['molecule']["identifiers"]["molecule_hash"] == chain1[6].get_hash()
    assert recs[0]["initial_chain"][-1]['molecule']["identifiers"]["molecule_hash"] == chain1[-1].get_hash()

    assert recs[1]["initial_chain"][0]['molecule']["identifiers"]["molecule_hash"] == chain2[0].get_hash()
    assert recs[1]["initial_chain"][6]['molecule']["identifiers"]["molecule_hash"] == chain2[6].get_hash()
    assert recs[1]["initial_chain"][-1]['molecule']["identifiers"]["molecule_hash"] == chain2[-1].get_hash()


#TODO: Do I need this one?
def test_neb_socket_add_same_chains_diff_order(storage_socket: SQLAlchemySocket):
    spec = _test_specs[0]

    mol1 = load_molecule_data("neb/neb_C3H2N_5")
    mol2 = load_molecule_data("neb/neb_C3H2N_6")

    # Add a molecule separately
    _, mol_ids = storage_socket.molecules.add([mol1])

    # Now add records
    meta, id = storage_socket.records.neb.add(
        [[mol1, mol2], [mol2, mol1]], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.success
    assert meta.n_inserted == 2
    #assert meta.n_existing == 2

    recs = storage_socket.records.neb.get(id, include=["initial_chain"])
    assert len(recs) == 2
    assert recs[0]["id"] != recs[1]["id"]



def test_neb_socket_add_same_1(storage_socket: SQLAlchemySocket):
    spec = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
            program="psi4",
            keywords={"k": "value"},
            driver="gradient",
            method="CCSD(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all"),
            ),
    )
    chain1 = [load_molecule_data("neb/neb_NCH_%i" %i) for i in range(43)]

    meta, id1 = storage_socket.records.neb.add(
        [chain1], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.neb.add(
        [chain1], spec, tag="*", priority=PriorityEnum.normal
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


#def test_neb_socket_add_same_2(storage_socket: SQLAlchemySocket):
#    # multiple molecule ordering, and duplicate molecules
#    spec = NEBSpecification(
#        program="geometric",
#        keywords=NEBKeywords(
#            images=11,
#            spring_constant=0.1,
#            energy_weighted=False,
#        ),
#        qc_specification=QCSpecification(
#            program="psi4",
#            keywords={"k": "value"},
#            driver="gradient",
#            method="CCSD(T)",
#            basis="def2-tzvp",
#            protocols=SinglepointProtocols(wavefunction="all"),
#            ),
#    )
#
#    mol1 = load_molecule_data("neb_C9H11NO2_0")
#    mol2 = load_molecule_data("neb_C9H11NO2_1")
#    mol3 = load_molecule_data("neb_C9H11NO2_2")
#    meta, id1 = storage_socket.records.neb.add(
#        [[mol1, mol2, mol3]], spec, tag="*", priority=PriorityEnum.normal, as_service=True
#    )
#    assert meta.n_inserted == 1
#    assert meta.inserted_idx == [0]
#
#    meta, id2 = storage_socket.records.neb.add(
#        [[mol2, mol3, mol1, mol2], [mol3, mol2, mol1, mol1]],
#        spec,
#        tag="*",
#        priority=PriorityEnum.normal,
#    )
#    assert meta.n_inserted == 0
#    assert meta.n_existing == 2
#    assert meta.existing_idx == [0, 1]
#    assert id2 == [id1[0], id1[0]]
#
#
def test_neb_socket_add_same_3(storage_socket: SQLAlchemySocket):
    # some modifications to the input specification
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
            program="psi4",
            keywords={"k": "value"},
            driver="gradient",
            method="CCSD(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all"),
            ),
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=0.1,
            energy_weighted=False,
        ),
        qc_specification=QCSpecification(
            program="PSI4",
            keywords={"k": "value"},
            driver="gradient",
            method="CCSD(T)",
            basis="def2-tzvp",
            protocols=SinglepointProtocols(wavefunction="all", stdout=True),
            ),
    )

    chain1 = [load_molecule_data("neb/neb_NCH_%i" %i) for i in range(43)]

    meta, id1 = storage_socket.records.neb.add(
        [chain1], spec1, tag="*", priority=PriorityEnum.normal
    )
    assert meta.n_inserted == 1
    assert meta.inserted_idx == [0]

    meta, id2 = storage_socket.records.neb.add(
        [chain1], spec2, tag="*", priority=PriorityEnum.normal
    )
    assert meta.n_inserted == 0
    assert meta.n_existing == 1
    assert meta.existing_idx == [0]
    assert id1 == id2


#def test_neb_socket_add_different_1(storage_socket: SQLAlchemySocket):
#    # Molecules are a subset of another
#    spec1 = NEBSpecification(
#        program="geometric",
#        keywords=NEBKeywords(
#            images=11,
#            spring_constant=0.1,
#            energy_weighted=False,
#        ),
#        qc_specification=QCSpecification(
#            program="psi4",
#            keywords={"k": "value"},
#            driver="gradient",
#            method="CCSD(T)",
#            basis="def2-tzvp",
#            protocols=SinglepointProtocols(wavefunction="all"),
#            ),
#    )
#
#    mol1 = load_molecule_data("neb_C9H11NO2_0")
#    mol2 = load_molecule_data("neb_C9H11NO2_1")
#    mol3 = load_molecule_data("neb_C9H11NO2_2")
#    meta, id1 = storage_socket.records.neb.add(
#        [[mol1, mol2, mol3]], spec, tag="*", priority=PriorityEnum.normal
#    )
#    assert meta.n_inserted == 1
#    assert meta.inserted_idx == [0]
#
#    meta, id2 = storage_socket.records.neb.add(
#        [[mol1], [mol3, mol2], [mol2, mol3, mol1]], spec, tag="*", priority=PriorityEnum.normal
#    )
#    assert meta.n_inserted == 2
#    assert meta.n_existing == 1
#    assert meta.existing_idx == [2]
#    assert meta.inserted_idx == [0, 1]
#    assert id1[0] == id2[2]
#
#
#def test_neb_socket_query(storage_socket: SQLAlchemySocket):
#    input_spec_1, molecules_1, result_data_1 = load_procedure_data("test")
    #input_spec_2, molecules_2, result_data_2 = load_procedure_data("td_H2O2_psi4_pbe")
    #input_spec_3, molecules_3, result_data_3 = load_procedure_data("td_C9H11NO2_psi4_b3lyp-d3bj")
    #input_spec_4, molecules_4, result_data_4 = load_procedure_data("td_H2O2_psi4_bp86")

   # meta_1, id_1 = storage_socket.records.neb.add(
   #     [molecules_1], input_spec_1, tag="*", priority=PriorityEnum.normal
   # )
    #meta_2, id_2 = storage_socket.records.neb.add(
    #    [molecules_2], input_spec_2, tag="*", priority=PriorityEnum.normal
    #)
    #meta_3, id_3 = storage_socket.records.neb.add(
    #    [molecules_3], input_spec_3, tag="*", priority=PriorityEnum.normal
    #)
    #meta_4, id_4 = storage_socket.records.neb.add(
    #    [molecules_4], input_spec_4, tag="*", priority=PriorityEnum.normal
    #)
#    assert meta_1.success #and meta_2.success and meta_3.success and meta_4.success
#
#    meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_program=["psi4"]))
#    assert meta.n_found == 1 #4
#
#    meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_program=["nothing"]))
#    assert meta.n_found == 0
#
    #_, init_mol_id = storage_socket.molecules.add(molecules_1 + molecules_2 + molecules_3 + molecules_4)
    #meta, neb = storage_socket.records.neb.query(
    #    NEBQueryBody(initial_molecule_id=[init_mol_id[0], 9999])
    #)
    #assert meta.n_found == 3

    # query for qc program
#    meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_program=["geometric"]))
#    assert meta.n_found == 1 #4
#
#    # query for qc program
#    meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_program=["geometric123"]))
#    assert meta.n_found == 0
#
    # query for basis
    #meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_basis=["sTO-3g"]))
    #assert meta.n_found == 3

    #meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_basis=[None]))
    #assert meta.n_found == 0

    #meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_basis=[""]))
    #assert meta.n_found == 0

    ## query for method
    #meta, neb = storage_socket.records.neb.query(NEBQueryBody(qc_method=["b3lyP"]))
    #assert meta.n_found == 1

    ## Query by default returns everything
    #meta, neb = storage_socket.records.neb.query(NEBQueryBody())
    #assert meta.n_found == 4

    ## Query by default (with a limit)
    #meta, neb = storage_socket.records.neb.query(NEBQueryBody(limit=1))
    #assert meta.n_found == 4
    #assert meta.n_returned == 1

#
#@pytest.mark.parametrize(
#    "test_data_name",
#    [
#        "neb_C9H11NO2_psi4_b3lyp-d3bj",
#        "neb_H2O2_psi4_b3lyp-d3bj",
#        "neb_H2O2_psi4_b3lyp",
#        "neb_H2O2_psi4_blyp",
#        "neb_H2O2_psi4_bp86",
#        "neb_H2O2_psi4_hf",
#        "neb_H2O2_psi4_pbe0-d3bj",
#        "neb_H2O2_psi4_pbe0",
#        "neb_H2O2_psi4_pbe",
#    ],
#)
#def test_neb_socket_run(storage_socket: SQLAlchemySocket, test_data_name: str):
#    input_spec_1, molecules_1, result_data_1 = load_procedure_data(test_data_name)
#
#    meta_1, id_1 = storage_socket.records.neb.add(
#        [molecules_1], input_spec_1, tag="test_tag", priority=PriorityEnum.low, as_service=True
#    )
#    assert meta_1.success
#
#    time_0 = datetime.utcnow()
#    finished, n_qcs = run_service_constropt(id_1[0], result_data_1, storage_socket, 200)
#    time_1 = datetime.utcnow()
#
#    rec = storage_socket.records.neb.get(
#        id_1,
#        include=[
#            "*",
#            "compute_history.*",
#            "compute_history.outputs",
#            "qcs.*",
#            "qcs.qc_record",
#            "service",
#        ],
#    )
#
#    assert rec[0]["status"] == RecordStatusEnum.complete
#    assert time_0 < rec[0]["modified_on"] < time_1
#    assert len(rec[0]["compute_history"]) == 1
#    assert len(rec[0]["compute_history"][-1]["outputs"]) == 1
#    assert rec[0]["compute_history"][-1]["status"] == RecordStatusEnum.complete
#    assert time_0 < rec[0]["compute_history"][-1]["modified_on"] < time_1
#    assert rec[0]["service"] is None
#    out = OutputStore(**rec[0]["compute_history"][-1]["outputs"]["snebout"])
#    assert "Job Finished" in out.as_string
#
#    assert len(rec[0]["qcs"]) == n_qcs
