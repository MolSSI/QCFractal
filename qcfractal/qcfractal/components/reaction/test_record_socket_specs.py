from qcfractal.components.testing_fixtures import spec_test_runner
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
