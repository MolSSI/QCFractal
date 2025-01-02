from qcarchivetesting import load_hash_test_data
from qcfractal.components.testing_fixtures import spec_test_runner
from qcfractal.components.torsiondrive.record_db_models import TorsiondriveSpecificationORM
from qcfractal.db_socket import SQLAlchemySocket
from qcportal.optimization import OptimizationSpecification, OptimizationProtocols
from qcportal.singlepoint import QCSpecification, SinglepointDriver, SinglepointProtocols
from qcportal.torsiondrive import TorsiondriveSpecification, TorsiondriveKeywords

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


def test_torsiondrive_hash_canaries(storage_socket: SQLAlchemySocket):
    # Test data is hash : spec dict
    test_data = load_hash_test_data("torsiondrive_specification_tests")

    # Hashes are independent of opt spec
    # TODO - protocols are not part of model
    for t in test_data.values():
        del t["protocols"]
    spec_map = [
        (k, TorsiondriveSpecification(optimization_specification=common_opt_spec, **v)) for k, v in test_data.items()
    ]

    specs = [x[1] for x in spec_map]
    meta, ids = storage_socket.records.torsiondrive.add_specifications(specs)
    assert meta.success
    assert len(ids) == len(specs)
    assert meta.n_existing == 0

    with storage_socket.session_scope() as session:
        for spec_id, (spec_hash, _) in zip(ids, spec_map):
            spec_orm = session.get(TorsiondriveSpecificationORM, spec_id)
            assert spec_orm.specification_hash == spec_hash


def test_torsiondrive_socket_add_specification_same_1(spec_test_runner):
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    spec_test_runner("torsiondrive", spec1, spec1, True)


def test_torsiondrive_socket_add_specification_same_2(spec_test_runner):
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    # model handling defaults
    spec2 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )
    spec_test_runner("torsiondrive", spec1, spec2, True)


def test_torsiondrive_socket_add_specification_same_3(spec_test_runner):
    # some changes to the opt spec
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    # model handling defaults
    spec2 = TorsiondriveSpecification(
        program="torSIOndrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            energy_upper_limit=0.05,
        ),
        optimization_specification=OptimizationSpecification(
            program="optpROg2",
            keywords={"k": "value"},
            protocols=OptimizationProtocols(trajectory="none"),
            qc_specification=QCSpecification(
                program="PROG2",
                driver=SinglepointDriver.gradient,
                method="hf",
                basis="def2-tZVp",
                keywords={"k": "value"},
                protocols=SinglepointProtocols(wavefunction="orbitals_and_eigenvalues"),
            ),
        ),
    )
    spec_test_runner("torsiondrive", spec1, spec2, True)


def test_torsiondrive_socket_add_specification_diff_1(spec_test_runner):
    #  changing energy upper limit
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    td_kw = spec1.keywords.copy(update={"energy_upper_limit": 0.051})
    spec2 = spec1.copy(update={"keywords": td_kw})
    spec_test_runner("torsiondrive", spec1, spec2, False)


def test_torsiondrive_socket_add_specification_diff_2(spec_test_runner):
    #  ordering of dihedrals
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    td_kw = spec1.keywords.copy(update={"dihedrals": [(8, 11, 13, 15)]})
    spec2 = spec1.copy(update={"keywords": td_kw})
    spec_test_runner("torsiondrive", spec1, spec2, False)


def test_torsiondrive_socket_add_specification_diff_3(spec_test_runner):
    #  grid spacing
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    td_kw = spec1.keywords.copy(update={"grid_spacing": [30]})
    spec2 = spec1.copy(update={"keywords": td_kw})
    spec_test_runner("torsiondrive", spec1, spec2, False)


def test_torsiondrive_socket_add_specification_diff_4(spec_test_runner):
    #  energy decrease thresh
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=None,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    td_kw = spec1.keywords.copy(update={"energy_decrease_thresh": 0.051})
    spec2 = spec1.copy(update={"keywords": td_kw})
    spec_test_runner("torsiondrive", spec1, spec2, False)


def test_torsiondrive_socket_add_specification_diff_5(spec_test_runner):
    # energy_upper_limit
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=0.2,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    td_kw = spec1.keywords.copy(update={"energy_upper_limit": 0.051})
    spec2 = spec1.copy(update={"keywords": td_kw})
    spec_test_runner("torsiondrive", spec1, spec2, False)


def test_torsiondrive_socket_add_specification_diff_6(spec_test_runner):
    # optimization keywords
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=0.2,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    opt_spec = spec1.optimization_specification.copy(update={"keywords": {"a": 1.0}})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("torsiondrive", spec1, spec2, False)


def test_torsiondrive_socket_add_specification_diff_7(spec_test_runner):
    # basis set
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=0.2,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.optimization_specification.qc_specification.copy(update={"basis": "def2-qzvp"})
    opt_spec = spec1.optimization_specification.copy(update={"qc_specification": qc_spec})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("torsiondrive", spec1, spec2, False)


def test_torsiondrive_socket_add_specification_diff_8(spec_test_runner):
    # qc keywords
    spec1 = TorsiondriveSpecification(
        program="torsiondrive",
        keywords=TorsiondriveKeywords(
            dihedrals=[(8, 11, 15, 13)],
            grid_spacing=[15],
            dihedral_ranges=None,
            energy_decrease_thresh=0.2,
            energy_upper_limit=0.05,
        ),
        optimization_specification=common_opt_spec,
    )

    qc_spec = spec1.optimization_specification.qc_specification.copy(update={"keywords": {"z": 1.0 - 10}})
    opt_spec = spec1.optimization_specification.copy(update={"qc_specification": qc_spec})
    spec2 = spec1.copy(update={"optimization_specification": opt_spec})
    spec_test_runner("torsiondrive", spec1, spec2, False)
