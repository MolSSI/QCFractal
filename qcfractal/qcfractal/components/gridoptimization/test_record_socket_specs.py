from qcarchivetesting.helpers import load_hash_test_data
from qcfractal.components.gridoptimization.record_db_models import GridoptimizationSpecificationORM
from qcfractal.components.testing_fixtures import spec_test_runner
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.gridoptimization import GridoptimizationSpecification, GridoptimizationKeywords
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols

common_opt_spec = OptimizationSpecification(
    program="optprog2",
    keywords={"k": "value"},
    protocols=OptimizationProtocols(trajectory="none"),
    qc_specification=QCSpecification(
        program="prog2",
        driver=SinglepointDriver.hessian,
        method="hf",
        basis="def2-tzvp",
        keywords={"k": "value"},
        protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
    ),
)


def test_gridoptimization_hash_canaries(storage_socket: SQLAlchemySocket):
    # Test data is hash : spec dict
    test_data = load_hash_test_data("gridoptimization_specification_tests")

    # Hashes are independent of opt spec
    # TODO - protocols are not part of model
    for t in test_data.values():
        del t["protocols"]
    spec_map = [
        (k, GridoptimizationSpecification(optimization_specification=common_opt_spec, **v))
        for k, v in test_data.items()
    ]

    specs = [x[1] for x in spec_map]
    meta, ids = storage_socket.records.gridoptimization.add_specifications(specs)
    assert meta.success
    assert len(ids) == len(specs)
    assert meta.n_existing == 0

    with storage_socket.session_scope() as session:
        for spec_id, (spec_hash, _) in zip(ids, spec_map):
            spec_orm = session.get(GridoptimizationSpecificationORM, spec_id)
            assert spec_orm.specification_hash == spec_hash


def test_gridoptimization_socket_add_specification_same_1(spec_test_runner):
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec_test_runner("gridoptimization", spec1, spec1, True)


def test_gridoptimization_socket_add_specification_same_2(spec_test_runner):
    # capitalization should be ignored
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec2 = GridoptimizationSpecification(
        program="gridopTIMIZatiON",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    spec_test_runner("gridoptimization", spec1, spec2, True)


def test_gridoptimization_socket_add_specification_diff_1(spec_test_runner):
    # different indices
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(
        update={"scans": [{"type": "distance", "indices": [0, 4], "steps": [2.0, 3.0, 4.0], "step_type": "relative"}]}
    )
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_2(spec_test_runner):
    # different steps
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(
        update={"scans": [{"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.1], "step_type": "relative"}]}
    )
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_3(spec_test_runner):
    # absolute vs relative
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(
        update={"scans": [{"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "absolute"}]}
    )
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_4(spec_test_runner):
    # preoptimization
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    go_kw = spec1.keywords.copy(update={"preoptimization": True})
    spec2 = spec1.copy(update={"keywords": go_kw})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_5(spec_test_runner):
    # basis set
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.optimization_specification.qc_specification.copy(update={"basis": "def2-qzvp"})
    opt_spec = spec1.optimization_specification.copy(update={"qc_specification": qc_spec})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("gridoptimization", spec1, spec2, False)


def test_gridoptimization_socket_add_specification_diff_6(spec_test_runner):
    # qc keywords
    spec1 = GridoptimizationSpecification(
        program="gridoptimization",
        keywords=GridoptimizationKeywords(
            preoptimization=False,
            scans=[
                {"type": "distance", "indices": [0, 3], "steps": [2.0, 3.0, 4.0], "step_type": "relative"},
            ],
        ),
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.optimization_specification.qc_specification.copy(update={"keywords": {"z": 1.0 - 10}})
    opt_spec = spec1.optimization_specification.copy(update={"qc_specification": qc_spec})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("gridoptimization", spec1, spec2, False)
