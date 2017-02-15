import numpy as np
import mongo_qcdb as mdb
from mongo_qcdb import test_util
import pytest

_water_dimer_minima = """
0 1
O  -1.551007  -0.114520   0.000000
H  -1.934259   0.762503   0.000000
H  -0.599677   0.040712   0.000000
--
0 1
O   1.350625   0.111469   0.000000
H   1.680398  -0.373741  -0.758561
H   1.680398  -0.373741   0.758561
"""

# Build a interesting database
@pytest.fixture
def water_db():
    db = mdb.Database("Water Data")

    # Build the water dimer.
    dimer = mdb.Molecule(_water_dimer_minima)
    frag_0 = dimer.get_fragment(0)
    frag_1 = dimer.get_fragment(1)
    frag_0_1 = dimer.get_fragment(0, 1)
    frag_1_0 = dimer.get_fragment(1, 0)

    # Add single stoich rxn via list
    db.add_rxn("Water Dimer, nocp", [(dimer, 1.0), (frag_0, -1.0), (frag_1, -1.0)], attributes={"R": "Minima"})

    # Add single stoich rxn via hashes
    db.add_rxn("Water Dimer, nocp - hash", [(dimer.get_hash(), 1.0), (frag_0.get_hash(), -1.0), (frag_1.get_hash(), -1.0)], attributes={"R": "Minima"})

    # Add multi stoich reaction via dict
    with pytest.raises(KeyError):
        db.add_rxn(
            "Null", {
                "Null": [(dimer, 1.0)]
            })

    # nocp and cp water dimer
    db.add_rxn(
        "Water Dimer, all", {
            "cp": [(dimer, 1.0), (frag_0_1, -1.0), (frag_1_0, -1.0)],
            "default": [(dimer, 1.0), (frag_0, -1.0), (frag_1, -1.0)]
        },
        other_fields={"Something": "Other thing"})

    return db


def test_rxn_add(water_db):

    assert water_db.data["name"] == "Water Data"
    assert len(water_db.get_index()) == 3

    nocp_stoich_class = water_db.get_rxn("Water Dimer, nocp")["stoichiometry"]["default"]
    nocp_stoich_hash = water_db.get_rxn("Water Dimer, nocp - hash")["stoichiometry"]["default"]
    nocp_stoich_dict = water_db.get_rxn("Water Dimer, all")["stoichiometry"]["default"]

    # Check if both builds check out
    assert len(nocp_stoich_class) == len(nocp_stoich_hash)
    for k in list(nocp_stoich_class):
        assert nocp_stoich_class[k] == nocp_stoich_hash[k]

    assert len(nocp_stoich_class) == len(nocp_stoich_dict)
    for k in list(nocp_stoich_class):
        assert nocp_stoich_class[k] == nocp_stoich_dict[k]





