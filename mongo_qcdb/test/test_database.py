import numpy as np
import pandas as pd
import os

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

_neon_trimer = """
Ne 0.000000 0.000000 0.000000
--
Ne 3.000000 0.000000 0.000000
--
Ne 0.000000 3.000000 0.000000
--
Ne 0.000000 0.000000 3.000000
"""


def _compare_stoichs(stoich, stoich_other):
    mols = list(stoich)
    mols_other = list(stoich_other)
    assert test_util.compare_lists(mols, mols_other)

    for mol in mols:
        assert stoich[mol] == stoich_other[mol]

    return True


def _compare_rxn_stoichs(ref, new):
    stoich = ref["stoichiometry"]
    stoich_other = new["stoichiometry"]

    keys = list(stoich)
    keys_other = list(stoich_other)
    assert test_util.compare_lists(keys, keys_other)

    for k in keys:
        _compare_stoichs(stoich[k], stoich_other[k])

    return True


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
    db.add_rxn(
        "Water Dimer, nocp", [(dimer, 1.0), (frag_0, -1.0), (frag_1, -1.0)],
        attributes={"R": "Minima"},
        return_values={"Benchmark": -20.0,
                       "DFT": -10.0})

    # Add single stoich rxn via hashes
    db.add_rxn(
        "Water Dimer, nocp - hash",
        [(dimer.get_hash(), 1.0), (frag_0.get_hash(), -1.0), (frag_1.get_hash(), -1.0)],
        attributes={"R": "Minima"},
        return_values={"Benchmark": -5.0})

    # Add multi stoich reaction via dict
    with pytest.raises(KeyError):
        db.add_rxn("Null", {"Null": [(dimer, 1.0)]})

    # nocp and cp water dimer
    db.add_rxn(
        "Water Dimer, all", {
            "cp": [(dimer, 1.0), (frag_0_1, -1.0), (frag_1_0, -1.0)],
            "default": [(dimer, 1.0), (frag_0, -1.0), (frag_1, -1.0)]
        },
        other_fields={"Something": "Other thing"})

    db.add_ie_rxn("Water dimer", _water_dimer_minima)

    return db


# Build a nbody database
@pytest.fixture
def nbody_db():
    db = mdb.Database("N-Body Data")

    dimer = mdb.Molecule(_water_dimer_minima)
    frag_0 = dimer.get_fragment(0)
    frag_1 = dimer.get_fragment(1)
    frag_0_1 = dimer.get_fragment(0, 1)
    frag_1_0 = dimer.get_fragment(1, 0)

    db.add_rxn("Water Dimer, bench", {
        "cp1": [(frag_0_1, 1.0), (frag_1_0, 1.0)],
        "default1": [(frag_0, 1.0), (frag_1, 1.0)],
        "cp": [(dimer, 1.0)],
        "default": [(dimer, 1.0)],
    })

    db.add_ie_rxn("Water Dimer", _water_dimer_minima)
    db.add_ie_rxn("Ne Tetramer", _neon_trimer)

    db.ne_stoich = {
        'default2': {
            '36f2143d90ae580e36557a7fc5143291c107eb97': 3.0,
            '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': -8.0,
            '1949decd4a49d09fee4327de6f1a3b855c90a5b5': 3.0
        },
        'cp3': {
            '9e3c887146748834ad617011b1d9fef2ec955ade': 1.0,
            '3f5157e975c337a1a5df85c5b63df1e6dc05600b': 1.0,
            'ea39591390e8b4482b190b666ebb79097020eb67': -1.0,
            'b760612638045cd544ff3cc41f0d5984802d84fd': 1.0,
            '394e88345c597385c61b3b14a89db7b40e7e70cf': 1.0,
            'a6cabbaa3f2e16b35ac11d54c8ad8581af53c183': -1.0,
            '3b08ee7157af42f3f92d5ad7524dc33b425181ae': -1.0,
            '3f30654fdc0c88ce90e10134720d773196aaf53b': -1.0,
            '7dfa7330f7c142974cc503ec1bac28f340c8c260': 1.0,
            'b76f32238bc0d7c5d188a258059c3ad827e80003': -1.0,
            '622bc2c6bfe57c87493fafec50fad5cc1227e10b': 1.0,
            '369552f3f1e2a49ef557159d1833e36e6ab8fdda': 1.0,
            '061fceb60603ae4de4abd72e3edec768c57b6619': -1.0,
            'a6628d1d05f5ee820e6bcbc75bcc09f0a599ca7b': 1.0
        },
        'cp': {
            'd5382fb286bdcc52fbb43c3cd355b5beba11436c': 1.0
        },
        'default3': {
            '36f2143d90ae580e36557a7fc5143291c107eb97': -3.0,
            '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': 4.0,
            'c224127cb1b1b2657063cd5686622d6c472d7c5b': 3.0,
            '1949decd4a49d09fee4327de6f1a3b855c90a5b5': -3.0,
            'e7ff4b2ba7b22190911757ca8cd576c84e480b69': 1.0
        },
        'cp2': {
            'b76f32238bc0d7c5d188a258059c3ad827e80003': 1.0,
            '3f5157e975c337a1a5df85c5b63df1e6dc05600b': -2.0,
            'a6cabbaa3f2e16b35ac11d54c8ad8581af53c183': 1.0,
            'ea39591390e8b4482b190b666ebb79097020eb67': 1.0,
            '622bc2c6bfe57c87493fafec50fad5cc1227e10b': -2.0,
            '369552f3f1e2a49ef557159d1833e36e6ab8fdda': -2.0,
            '061fceb60603ae4de4abd72e3edec768c57b6619': 1.0,
            '9e3c887146748834ad617011b1d9fef2ec955ade': -2.0,
            '3b08ee7157af42f3f92d5ad7524dc33b425181ae': 1.0,
            '3f30654fdc0c88ce90e10134720d773196aaf53b': 1.0
        },
        'cp1': {
            '3f5157e975c337a1a5df85c5b63df1e6dc05600b': 1.0,
            '9e3c887146748834ad617011b1d9fef2ec955ade': 1.0,
            '622bc2c6bfe57c87493fafec50fad5cc1227e10b': 1.0,
            '369552f3f1e2a49ef557159d1833e36e6ab8fdda': 1.0
        },
        'default1': {
            '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': 4.0
        },
        'default': {
            'd5382fb286bdcc52fbb43c3cd355b5beba11436c': 1.0
        }
    }

    return db


# Build HBC from dataframe
@pytest.fixture
def hbc_from_df():

    fn = os.path.abspath(os.path.dirname(__file__)) + "/../../databases/DB_HBC6/HBC6.pd_pkl"
    df = pd.read_pickle(fn)

    db = mdb.Database("HBC 6", db_type="ie")

    for idx, row in df.iterrows():

        rvals = {}
        rvals["cp"] = {}
        rvals["default"] = {}

        datacols = [x for x in row.index if ("CP" in x) or ("noCP" in x)]
        for col in datacols:
            name = col.replace("-CP-", "/")
            name = name.replace("-noCP-", "/")
            name = name.replace("adz", "aug-cc-pVDZ")
            name = name.replace("qzvp", "def2-QZVP")

            if "-CP-" in col:
                rvals["cp"][name] = row[col]
            else:
                rvals["default"][name] = row[col]

        rvals["cp"]["Benchmark"] = row["Benchmark"]
        rvals["default"]["Benchmark"] = row["Benchmark"]

        for col in [
                'SAPT DISP ENERGY', 'SAPT ELST ENERGY', 'SAPT EXCH ENERGY', 'SAPT IND ENERGY',
                'SAPT TOTAL ENERGY'
        ]:
            rvals["default"][col] = row[col]

        name = row["System"].strip() + " " + str(round(row["R"], 2))
        db.add_ie_rxn(
            name,
            row["Geometry"],
            dtype="numpy",
            frags=[row["MonA"]],
            attribute={"R": row["R"]},
            return_values=rvals)

    return db


# Test conventional add
def test_rxn_add(water_db):

    assert water_db.data["name"] == "Water Data"
    assert len(water_db.get_index()) == 4

    nocp_stoich_class = water_db.get_rxn("Water Dimer, nocp")["stoichiometry"]["default"]
    nocp_stoich_hash = water_db.get_rxn("Water Dimer, nocp - hash")["stoichiometry"]["default"]
    nocp_stoich_dict = water_db.get_rxn("Water Dimer, all")["stoichiometry"]["default"]

    # Check if both builds check out
    _compare_stoichs(nocp_stoich_class, nocp_stoich_hash)
    _compare_stoichs(nocp_stoich_class, nocp_stoich_dict)


# Test IE add
def test_nbody_rxn(nbody_db):

    # Check the Water Dimer
    water_stoich_bench = nbody_db.get_rxn("Water Dimer, bench")["stoichiometry"]
    water_stoich = nbody_db.get_rxn("Water Dimer")["stoichiometry"]
    _compare_stoichs(water_stoich, water_stoich_bench)

    # Check the N-body
    ne_stoich = nbody_db.get_rxn("Ne Tetramer")["stoichiometry"]


# Test dataframe
def test_dataframe_return_values(water_db):

    assert water_db.df.ix["Water Dimer, nocp", "Benchmark"] == -20.0
    assert water_db.df.ix["Water Dimer, nocp", "DFT"] == -10.0
    assert water_db.df.ix["Water Dimer, nocp - hash", "Benchmark"] == -5.0

    assert np.isnan(water_db.df.ix["Water dimer", "Benchmark"])


def test_dataframe_stats(hbc_from_df):

    # Remap
    db = hbc_from_df

    # Check the stats
    assert np.allclose(0.7462906, db.statistics("ME", "B3LYP/aug-cc-pVDZ"), atol=1.e-5)
    assert np.allclose(0.7467296, db.statistics("MUE", "B3LYP/aug-cc-pVDZ"), atol=1.e-5)
    assert np.allclose(6.8810951, db.statistics("MURE", "B3LYP/aug-cc-pVDZ"), atol=1.e-5)
    assert np.allclose(
        [6.8810951, 8.878373],
        db.statistics("MURE", ["B3LYP/aug-cc-pVDZ", "B3LYP/def2-QZVP"]),
        atol=1.e-5)
    assert np.allclose(
        -0.263942, db.statistics(
            "ME", "B3LYP/aug-cc-pVDZ", bench="B3LYP/def2-QZVP"), atol=1.e-5)
    assert np.allclose(
        -0.263942,
        db.statistics(
            "ME", db["B3LYP/aug-cc-pVDZ"], bench="B3LYP/def2-QZVP"),
        atol=1.e-5)

def test_dataframe_visualization(hbc_from_df):

    # Remap
    db = hbc_from_df

    # No return value to test
    hbc_from_df.ternary()
