from qcarchivetesting import load_hash_test_data
from qcfractal.components.manybody.record_db_models import ManybodySpecificationORM
from qcfractal.components.testing_fixtures import spec_test_runner
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.manybody import ManybodySpecification
from qcportal.singlepoint import SinglepointProtocols, QCSpecification


def test_manybody_hash_canaries(storage_socket: SQLAlchemySocket):
    # Test data is hash : spec dict
    test_data = load_hash_test_data("manybody_specification_tests")

    spec_map = [(k, ManybodySpecification(**v)) for k, v in test_data.items()]

    specs = [x[1] for x in spec_map]
    meta, ids = storage_socket.records.manybody.add_specifications(specs)
    assert meta.success
    assert len(ids) == len(specs)
    assert meta.n_existing == 0

    with storage_socket.session_scope() as session:
        for spec_id, (spec_hash, _) in zip(ids, spec_map):
            spec_orm = session.get(ManybodySpecificationORM, spec_id)
            assert spec_orm.specification_hash == spec_hash


def test_manybody_socket_add_specification_same_1(spec_test_runner):
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec_test_runner("manybody", spec1, spec1, True)


def test_manybody_socket_add_specification_same_2(spec_test_runner):
    # Test case sensitivity
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prOg1",
                driver="energy",
                method="b3LYP",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec_test_runner("manybody", spec1, spec2, True)


def test_manybody_socket_add_specification_same_3(spec_test_runner):
    # Test supersystem
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            "supersystem": QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec_test_runner("manybody", spec1, spec1, True)


def test_manybody_socket_add_specification_same_4(spec_test_runner):
    # Test ordering
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            "supersystem": QCSpecification(
                program="prog1",
                driver="energy",
                method="ccsd",
                basis="aug-cc-pvtz",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["nocp"],
        keywords={},
    )

    # Test ordering
    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            "supersystem": QCSpecification(
                program="prog1",
                driver="energy",
                method="ccsd",
                basis="aug-cc-pvtz",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="sto-3g",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["nocp"],
        keywords={},
    )

    spec_test_runner("manybody", spec1, spec2, True)


def test_manybody_socket_add_specification_diff_1(spec_test_runner):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec_test_runner("manybody", spec1, spec2, False)


def test_manybody_socket_add_specification_diff_2(spec_test_runner):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["nocp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["vmfc"],
        keywords={"return_total_data": True},
    )

    spec_test_runner("manybody", spec1, spec2, False)


def test_manybody_socket_add_specification_diff_3(spec_test_runner):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": False},
    )
    spec_test_runner("manybody", spec1, spec2, False)


def test_manybody_socket_add_specification_diff_4(spec_test_runner):
    # Test different parameters
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            )
        },
        bsse_correction=["cp"],
    )
    spec_test_runner("manybody", spec1, spec2, False)


def test_manybody_socket_add_specification_diff_5(spec_test_runner):
    # Test different levels
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={},
    )

    spec_test_runner("manybody", spec1, spec2, False)


def test_manybody_socket_add_specification_diff_6(spec_test_runner):
    # Test different levels
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="ccsd",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec_test_runner("manybody", spec1, spec2, False)


def test_manybody_socket_add_specification_diff_7(spec_test_runner):
    # Test different levels
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="none"),
            ),
        },
        bsse_correction=["cp"],
        keywords={"return_total_data": True},
    )
    spec_test_runner("manybody", spec1, spec2, False)


def test_manybody_socket_add_specification_diff_8(spec_test_runner):
    # Test different levels
    spec1 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="ccsd",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            2: QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
    )

    spec2 = ManybodySpecification(
        program="qcmanybody",
        levels={
            1: QCSpecification(
                program="prog1",
                driver="energy",
                method="b3lyp",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
            "supersystem": QCSpecification(
                program="prog1",
                driver="energy",
                method="hf",
                basis="6-31G*",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="all"),
            ),
        },
        bsse_correction=["cp"],
    )

    spec_test_runner("manybody", spec1, spec2, False)
