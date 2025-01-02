from qcarchivetesting import load_hash_test_data
from qcfractal.components.neb.record_db_models import NEBSpecificationORM
from qcfractal.components.testing_fixtures import spec_test_runner
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.neb import NEBSpecification, NEBKeywords
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols

common_sp_spec = QCSpecification(
    program="psi4",
    method="CCSD(T)",
    basis="def2-tzvp",
    keywords={"k1": "values1"},
    driver=SinglepointDriver.gradient,
    protocols=SinglepointProtocols(wavefunction="all"),
)

common_opt_spec = OptimizationSpecification(
    program="optprog2",
    keywords={"k": "value"},
    protocols=OptimizationProtocols(trajectory="none"),
    qc_specification=common_sp_spec.copy(),
)


def test_neb_hash_canaries(storage_socket: SQLAlchemySocket):
    # Test data is hash : spec dict
    test_data = load_hash_test_data("neb_specification_tests")

    # Hashes are independent of opt and sp spec
    # TODO - protocols are not part of model
    for t in test_data.values():
        del t["protocols"]
    spec_map = [
        (k, NEBSpecification(singlepoint_specification=common_sp_spec, optimization_specification=common_opt_spec, **v))
        for k, v in test_data.items()
    ]

    specs = [x[1] for x in spec_map]
    meta, ids = storage_socket.records.neb.add_specifications(specs)
    assert meta.success
    assert len(ids) == len(specs)
    assert meta.n_existing == 0

    with storage_socket.session_scope() as session:
        for spec_id, (spec_hash, _) in zip(ids, spec_map):
            spec_orm = session.get(NEBSpecificationORM, spec_id)
            assert spec_orm.specification_hash == spec_hash


def test_neb_socket_add_specification_same_1(spec_test_runner):
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(images=11, spring_constant=1.0, optimize_ts=True, optimize_endpoints=True),
        singlepoint_specification=common_sp_spec,
    )

    spec_test_runner("neb", spec1, spec1, True)


def test_neb_socket_add_specification_same_2(spec_test_runner):
    # model handling defaults
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(images=11, spring_constant=1.0, optimize_ts=False),
        singlepoint_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(),
        singlepoint_specification=common_sp_spec,
    )

    spec_test_runner("neb", spec1, spec2, True)


def test_neb_socket_add_specification_same_3(spec_test_runner):
    # Test keywords defaults
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(),
        singlepoint_specification=common_sp_spec,
    )

    spec2 = NEBSpecification(
        program="geometric", keywords={}, singlepoint_specification=common_sp_spec, optimization_specification=None
    )

    spec_test_runner("neb", spec1, spec2, True)


def test_neb_socket_add_specification_diff_1(spec_test_runner):
    # Specifying opt spec
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=11,
            spring_constant=1.0,
            spring_type=1,
        ),
        singlepoint_specification=common_sp_spec,
    )

    spec2 = spec1.copy(update={"optimization_specification": common_opt_spec})
    spec_test_runner("neb", spec1, spec2, False)


def test_neb_socket_add_specification_diff_2(spec_test_runner):
    # change images
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    neb_kw = spec1.keywords.copy(update={"images": 31})
    spec2 = spec1.copy(update={"keywords": neb_kw})
    spec_test_runner("neb", spec1, spec2, False)


def test_neb_socket_add_specification_diff_3(spec_test_runner):
    # change spring constant
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    neb_kw = spec1.keywords.copy(update={"spring_constant": 1.51})
    spec2 = spec1.copy(update={"keywords": neb_kw})
    spec_test_runner("neb", spec1, spec2, False)


def test_neb_socket_add_specification_diff_4(spec_test_runner):
    # change opt endpoints
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    neb_kw = spec1.keywords.copy(update={"optimize_endpoints": True})
    spec2 = spec1.copy(update={"keywords": neb_kw})
    spec_test_runner("neb", spec1, spec2, False)


def test_neb_socket_add_specification_diff_5(spec_test_runner):
    # change max force
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    neb_kw = spec1.keywords.copy(update={"maximum_force": 0.051})
    spec2 = spec1.copy(update={"keywords": neb_kw})
    spec_test_runner("neb", spec1, spec2, False)


def test_neb_socket_add_specification_diff_6(spec_test_runner):
    # change align
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
    )

    neb_kw = spec1.keywords.copy(update={"align": False})
    spec2 = spec1.copy(update={"keywords": neb_kw})
    spec_test_runner("neb", spec1, spec2, False)


def test_neb_socket_add_specification_diff_7(spec_test_runner):
    # change sp basis
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
        optimization_specification=common_opt_spec,
    )

    sp_spec = spec1.singlepoint_specification.copy(update={"basis": "def2-qzvp"})
    spec2 = spec1.copy(update={"singlepoint_specification": sp_spec})
    spec_test_runner("neb", spec1, spec2, False)


def test_neb_socket_add_specification_diff_8(spec_test_runner):
    # change opt protocols
    spec1 = NEBSpecification(
        program="geometric",
        keywords=NEBKeywords(
            images=21,
            spring_constant=1.5,
            optimize_endpoints=False,
            spring_type=0,
        ),
        singlepoint_specification=common_sp_spec,
        optimization_specification=common_opt_spec,
    )

    opt_spec = spec1.optimization_specification.copy(update={"protocols": OptimizationProtocols(trajectory="all")})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("neb", spec1, spec2, False)
