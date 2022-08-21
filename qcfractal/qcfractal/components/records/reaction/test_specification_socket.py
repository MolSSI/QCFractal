from qcfractal.db_socket import SQLAlchemySocket
from qcportal.records.optimization import OptimizationSpecification
from qcportal.records.reaction import ReactionSpecification
from qcportal.records.reaction.models import ReactionKeywords
from qcportal.records.singlepoint import SinglepointProtocols, QCSpecification


def test_reaction_socket_add_specification_same_0(storage_socket: SQLAlchemySocket):
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(),
            ),
            keywords={},
        ),
    )

    meta, id = storage_socket.records.reaction.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    # Try inserting again
    meta, id2 = storage_socket.records.reaction.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_reaction_socket_add_specification_same_1(storage_socket: SQLAlchemySocket):
    # Case sensitivity

    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(),
            ),
            keywords={},
        ),
    )

    spec2 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="pROG1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=OptimizationSpecification(
            program="geoMetric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3LYP",
                basis="sto-3G",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(),
            ),
            keywords={},
        ),
    )

    meta, id = storage_socket.records.reaction.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.reaction.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_reaction_socket_add_specification_same_2(storage_socket: SQLAlchemySocket):
    # Defaults

    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
                keywords={"k": "value"},
            ),
            keywords={},
        ),
    )

    spec2 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(),
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(),
            ),
            keywords={},
        ),
    )

    meta, id = storage_socket.records.reaction.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.reaction.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_reaction_socket_add_specification_same_3(storage_socket: SQLAlchemySocket):
    # Defaults

    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={},
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
            ),
            keywords={},
        ),
    )

    spec2 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
                keywords={},
            ),
            keywords={},
        ),
    )

    meta, id = storage_socket.records.reaction.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.reaction.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == []
    assert meta.existing_idx == [0]
    assert id == id2


def test_reaction_socket_add_specification_diff_1(storage_socket: SQLAlchemySocket):

    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(),
            ),
            keywords={},
        ),
    )

    spec2 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="pROG1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=None,
    )

    meta, id = storage_socket.records.reaction.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.reaction.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []


def test_reaction_socket_add_specification_diff_2(storage_socket: SQLAlchemySocket):
    # Differt qc method in opt

    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(),
            ),
            keywords={},
        ),
    )

    spec2 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=QCSpecification(
            program="prog1",
            driver="energy",
            method="b3lyp",
            basis="6-31G*",
            keywords={"k": "value"},
            protocols=SinglepointProtocols(wavefunction="all"),
        ),
        optimization_specification=OptimizationSpecification(
            program="geometric",
            qc_specification=QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(),
            ),
            keywords={},
        ),
    )

    meta, id = storage_socket.records.reaction.add_specification(spec1)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
    assert id is not None

    meta, id2 = storage_socket.records.reaction.add_specification(spec2)
    assert meta.success
    assert meta.inserted_idx == [0]
    assert meta.existing_idx == []
