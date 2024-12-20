from qcarchivetesting import load_hash_test_data
from qcfractal.components.reaction.record_db_models import ReactionSpecificationORM
from qcfractal.components.testing_fixtures import spec_test_runner
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.reaction import ReactionSpecification, ReactionKeywords
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols

common_sp_spec = QCSpecification(
    program="prog2",
    driver=SinglepointDriver.energy,
    method="",
    basis="def2-tzvp",
    keywords={"k": "value"},
    protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
)

common_opt_spec = OptimizationSpecification(
    program="optprog2",
    keywords={"k": "value"},
    protocols=OptimizationProtocols(trajectory="none"),
    qc_specification=common_sp_spec.copy(),
)


def test_reaction_hash_canaries(storage_socket: SQLAlchemySocket):
    # Test data is hash : spec dict
    test_data = load_hash_test_data("reaction_specification_tests")

    # Hashes are independent of opt and sp spec
    # TODO - protocols are not part of model
    for t in test_data.values():
        del t["protocols"]
    spec_map = [
        (
            k,
            ReactionSpecification(
                singlepoint_specification=common_sp_spec, optimization_specification=common_opt_spec, **v
            ),
        )
        for k, v in test_data.items()
    ]

    specs = [x[1] for x in spec_map]
    meta, ids = storage_socket.records.reaction.add_specifications(specs)
    assert meta.success
    assert len(ids) == len(specs)
    assert meta.n_existing == 0

    with storage_socket.session_scope() as session:
        for spec_id, (spec_hash, _) in zip(ids, spec_map):
            spec_orm = session.get(ReactionSpecificationORM, spec_id)
            assert spec_orm.specification_hash == spec_hash


def test_reaction_socket_add_specification_same_1(spec_test_runner):
    # both sp and opt
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=common_sp_spec,
        optimization_specification=common_opt_spec,
    )

    spec_test_runner("reaction", spec1, spec1, True)


def test_reaction_socket_add_specification_same_2(spec_test_runner):
    # only sp
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=common_sp_spec,
        optimization_specification=None,
    )

    spec_test_runner("reaction", spec1, spec1, True)


def test_reaction_socket_add_specification_same_3(spec_test_runner):
    # only opt
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=None,
        optimization_specification=common_opt_spec,
    )

    spec_test_runner("reaction", spec1, spec1, True)


def test_reaction_socket_add_specification_diff_1(spec_test_runner):
    # removing opt spec
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=common_sp_spec,
        optimization_specification=common_opt_spec,
    )

    spec2 = spec1.copy(update={"optimization_specification": None})
    spec_test_runner("reaction", spec1, spec2, False)


def test_reaction_socket_add_specification_diff_2(spec_test_runner):
    # removing qc spec
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=common_sp_spec,
        optimization_specification=common_opt_spec,
    )

    spec2 = spec1.copy(update={"singlepoint_specification": None})
    spec_test_runner("reaction", spec1, spec2, False)


def test_reaction_socket_add_specification_diff_3(spec_test_runner):
    # removing qc spec
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=None,
        optimization_specification=common_opt_spec,
    )

    spec2 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=common_sp_spec,
        optimization_specification=None,
    )

    spec_test_runner("reaction", spec1, spec2, False)


def test_reaction_socket_add_specification_diff_4(spec_test_runner):
    # different basis in qc spec
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=common_sp_spec,
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.singlepoint_specification.copy(update={"basis": "sto-3g"})
    spec2 = spec1.copy(update={"singlepoint_specification": qc_spec})
    spec_test_runner("reaction", spec1, spec2, False)


def test_reaction_socket_add_specification_diff_5(spec_test_runner):
    # different basis in opt spec
    spec1 = ReactionSpecification(
        program="reaction",
        keywords=ReactionKeywords(),
        singlepoint_specification=common_sp_spec,
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.optimization_specification.qc_specification.copy(update={"basis": "sto-3g"})
    opt_spec = spec1.optimization_specification.copy(update={"qc_specification": qc_spec})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("reaction", spec1, spec2, False)
