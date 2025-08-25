from qcportal.gridoptimization import compare_gridoptimization_records
from qcportal.manybody import compare_manybody_records
from qcportal.neb import compare_neb_records
from qcportal.optimization import compare_optimization_records
from qcportal.reaction import compare_reaction_records
from qcportal.singlepoint import compare_singlepoint_records
from qcportal.torsiondrive import compare_torsiondrive_records

_compare_map = {
    "singlepoint": compare_singlepoint_records,
    "optimization": compare_optimization_records,
    "gridoptimization": compare_gridoptimization_records,
    "torsiondrive": compare_torsiondrive_records,
    "reaction": compare_reaction_records,
    "manybody": compare_manybody_records,
    "neb": compare_neb_records,
}


def compare_records(rec_1, rec_2):
    assert rec_1.record_type == rec_2.record_type
    _compare_map[rec_1.record_type](rec_1, rec_2)
