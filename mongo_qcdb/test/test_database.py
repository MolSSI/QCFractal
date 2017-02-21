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
Ne 0.000000 4.000000 0.000000
--
Ne 0.000000 0.000000 5.000000
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

    # Add single stoich from strings, not a valid set
    db.add_rxn(
        "Water Dimer, dimer - str (invalid)",
        [(_water_dimer_minima, 1.0), (_water_dimer_minima.splitlines()[-1], 0.0)],
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

    # Nothing to fragment
    # with pytest.raises(AttributeError):
    #     db.add_ie_rxn("Water MonomerA", frag_0)

    # Ne Tetramer benchmark
    db.ne_stoich = {
        'attributes': {},
        'stoichiometry': {
            'default1': {
                '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': 4.0
            },
            'cp3': {
                'c809aab151bd9cbaa2750e764083fdbe6c1aabc9': -1.0,
                '3a0fa0c60401faa0693985fb242c91499edd277d': -1.0,
                'f291521beb55759ce0b2bb26591768a2ebfd5b37': 1.0,
                'e6e7529e74bfd80c9a75295ea89e40ee9b14158e': -1.0,
                'b45b5e2105344cb05124d220ab9c8dfa8c5e96be': 1.0,
                '6672d0ba44c4c0667991d06a5f6c85a3aabf37d0': 1.0,
                'b37cc789404c7da6055eee777750d9e03bd5e5c0': 1.0,
                '961859dc5db0bebf52da368a19f8918afc3162bf': -1.0,
                'a3e23dfff6751d4434dc358d50cfd23b78408dfc': 1.0,
                '4397c59cf960939df79b46661ab3f7c9598ba47b': 1.0,
                '2b1cd4c9b1081f904caabcca7f5d96b609adb3ae': 1.0,
                'b06cc88f3eec0f8c96bcd99ac9b02af5d89c8cd6': -1.0,
                '8d7a0111e3fd61565cafc5127605d903ecc97171': -1.0,
                'a09b9a4f3184ef569bcec6cd7fa8a424ee2dc172': 1.0
            },
            'cp2': {
                'c809aab151bd9cbaa2750e764083fdbe6c1aabc9': 1.0,
                '8d7a0111e3fd61565cafc5127605d903ecc97171': 1.0,
                '961859dc5db0bebf52da368a19f8918afc3162bf': 1.0,
                'a3e23dfff6751d4434dc358d50cfd23b78408dfc': -2.0,
                'f291521beb55759ce0b2bb26591768a2ebfd5b37': -2.0,
                'e6e7529e74bfd80c9a75295ea89e40ee9b14158e': 1.0,
                '3a0fa0c60401faa0693985fb242c91499edd277d': 1.0,
                'b06cc88f3eec0f8c96bcd99ac9b02af5d89c8cd6': 1.0,
                '2b1cd4c9b1081f904caabcca7f5d96b609adb3ae': -2.0,
                'a09b9a4f3184ef569bcec6cd7fa8a424ee2dc172': -2.0
            },
            'default': {
                '358e92f6323f88ed7385d2184c5564c012779bd7': 1.0
            },
            'default2': {
                '910c2e5fe9418644c3f50fe9141db2f06b3bafe5': 1.0,
                '0f617c87b2f4ff3c7b1076c6ec88ab59af0df360': 1.0,
                '01a496b0dad9b88c5a99430823877dd964e928d4': 1.0,
                '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': -8.0,
                '72ef2f6d853e67e3fab3682544b810d5ba8d69ef': 1.0,
                '3a767f68e03c3e58d8062d7105be2a358db0a9ea': 2.0
            },
            'cp1': {
                '2b1cd4c9b1081f904caabcca7f5d96b609adb3ae': 1.0,
                'a3e23dfff6751d4434dc358d50cfd23b78408dfc': 1.0,
                'f291521beb55759ce0b2bb26591768a2ebfd5b37': 1.0,
                'a09b9a4f3184ef569bcec6cd7fa8a424ee2dc172': 1.0
            },
            'cp': {
                '358e92f6323f88ed7385d2184c5564c012779bd7': 1.0
            },
            'default3': {
                'c0be8c7edc89b032e1c77781d96df7dea61b91d7': 1.0,
                '910c2e5fe9418644c3f50fe9141db2f06b3bafe5': -1.0,
                '0f617c87b2f4ff3c7b1076c6ec88ab59af0df360': -1.0,
                '01a496b0dad9b88c5a99430823877dd964e928d4': -1.0,
                '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': 4.0,
                '72ef2f6d853e67e3fab3682544b810d5ba8d69ef': -1.0,
                '51fbba67ae64dd0c99baeaeca8d3209986a99cd6': 1.0,
                '479812dafce9f952b897926dac26bd9a14d1a14d': 1.0,
                '3a767f68e03c3e58d8062d7105be2a358db0a9ea': -2.0,
                'c6edf571991f76bea288b4e155cfb0d07ed46f3a': 1.0
            }
        },
        'name': 'Ne Tetramer'
    }
    return db


# Build HBC from dataframe
@pytest.fixture(scope="module")
def hbc_from_df():

    fn = os.path.abspath(os.path.dirname(__file__)) + "/../../databases/DB_HBC6/HBC6.pd_pkl"
    df = pd.read_pickle(fn)

    db = mdb.Database("HBC_6", db_type="ie")

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
    assert len(water_db.get_index()) == 5

    nocp_stoich_class = water_db.get_rxn("Water Dimer, nocp")["stoichiometry"]["default"]
    nocp_stoich_hash = water_db.get_rxn("Water Dimer, nocp - hash")["stoichiometry"]["default"]
    nocp_stoich_dict = water_db.get_rxn("Water Dimer, all")["stoichiometry"]["default"]

    # Check if both builds check out
    _compare_stoichs(nocp_stoich_class, nocp_stoich_hash)
    _compare_stoichs(nocp_stoich_class, nocp_stoich_dict)


# Test IE add
def test_nbody_rxn(nbody_db):

    # Check the Water Dimer
    water_stoich_bench = nbody_db.get_rxn("Water Dimer, bench")
    water_stoich = nbody_db.get_rxn("Water Dimer")
    _compare_rxn_stoichs(water_stoich, water_stoich_bench)

    # Check the N-body
    ne_stoich = nbody_db.get_rxn("Ne Tetramer")
    mh = list(ne_stoich["stoichiometry"]["default"])[0]
    _compare_rxn_stoichs(nbody_db.ne_stoich, ne_stoich)


# Test dataframe
def test_dataframe_return_values(water_db):

    assert water_db.df.ix["Water Dimer, nocp", "Benchmark"] == -20.0
    assert water_db.df.ix["Water Dimer, nocp", "DFT"] == -10.0
    assert water_db.df.ix["Water Dimer, nocp - hash", "Benchmark"] == -5.0

    assert np.isnan(water_db.df.ix["Water dimer", "Benchmark"])


def test_dataframe_stats(hbc_from_df):

    # Remap
    db = hbc_from_df

    # Single value stats
    assert np.allclose(0.7462906, db.statistics("ME", "B3LYP/aug-cc-pVDZ"), atol=1.e-5)
    assert np.allclose(0.7467296, db.statistics("MUE", "B3LYP/aug-cc-pVDZ"), atol=1.e-5)
    assert np.allclose(6.8810951, db.statistics("MURE", "B3LYP/aug-cc-pVDZ"), atol=1.e-5)

    # Series return
    assert np.allclose(
        [6.8810951, 8.878373],
        db.statistics("MURE", ["B3LYP/aug-cc-pVDZ", "B3LYP/def2-QZVP"]),
        atol=1.e-5)
    assert np.allclose(
        [6.8810951, 8.878373],
        db.statistics("MURE", db[["B3LYP/aug-cc-pVDZ", "B3LYP/def2-QZVP"]]),
        atol=1.e-5)
    assert np.allclose(
        -0.263942, db.statistics(
            "ME", "B3LYP/aug-cc-pVDZ", bench="B3LYP/def2-QZVP"), atol=1.e-5)

    # Different benchmark
    assert np.allclose(
        -0.263942,
        db.statistics(
            "ME", db["B3LYP/aug-cc-pVDZ"], bench=db["B3LYP/def2-QZVP"]),
        atol=1.e-5)
    assert np.allclose(
        -0.263942,
        db.statistics(
            "ME", db["B3LYP/aug-cc-pVDZ"], bench=np.asarray(db["B3LYP/def2-QZVP"])),
        atol=1.e-5)


def test_dataframe_saving_loading(hbc_from_df):

    # Remap
    db = hbc_from_df

    mongo = mdb.db_helper.MongoSocket("127.0.0.1", 27017, "HBC6_tmp")
    db.save(mongo, name_override=True)


# Seg faults on travis
# def test_dataframe_visualization(hbc_from_df):

#     # Remap
#     db = hbc_from_df

#     # No return value to test
#     hbc_from_df.ternary()
