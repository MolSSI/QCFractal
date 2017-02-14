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
    
    dimer = mdb.Molecule(_water_dimer_minima) 
    frag_0 = dimer.get_fragment(0)
    frag_1 = dimer.get_fragment(1)
    frag_0_1 = dimer.get_fragment(0, 1)
    frag_1_0 = dimer.get_fragment(1, 0)
    
    db.add_rxn("Water Dimer nocp,", [(dimer, 1.0), (frag_0, -1.0), (frag_1, -1.0)], attributes={"R": "Minima"})
    
    db.add_rxn("Water Dimer nocp, hash", [(dimer.get_hash(), 1.0), (frag_0.get_hash(), -1.0), (frag_1.get_hash(), -1.0)], attributes={"R": "Minima"})
    return db


def test_rxn_molecule(water_db):
    
    assert water_db.data["name"] == "Water Data"
