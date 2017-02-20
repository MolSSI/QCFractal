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
        'stoichiometry': {
            'cp3': {
                'd189009ff65accb304fa062df7df876d208f0913': -1.0,
                '475a207e912aaf6c2575e8a9c7181eb7efa17396': 1.0,
                '4ce966e9eebce2badb2f224350a8856712faf1a7': 1.0,
                'f275f61a4bc7f9937d93d70f15d79f308001db6a': -1.0,
                'c441b65c480520f0cabc40fd02442f4c6c13ff65': -1.0,
                '9d0727c508b512695916d191d2e70ff287500cd8': -1.0,
                '621be25335fb2d707883c5e941279e5d52ccbfc0': 1.0,
                '52bffbb5977ca91a7ed804e0113a8089327d125b': 1.0,
                '7e4b34c6a7d5d336c74bf6396cbb488e2cafb1de': -1.0,
                '3aa65cf961182a1d20b12edb5b44f9b63b5d5881': 1.0,
                'cae082641e64bff597c76887e4283ce71f598b1c': 1.0,
                '232edb147cbcc67b762f45af402ef28dba21024a': 1.0,
                'e083d732a8049ac739d8cc0f8fa19964610a6819': -1.0,
                'a6a4c85bc3b6f466442537eda3a9b895dc1dd4bd': 1.0
            },
            'default': {
                'a61b76b5591909861d3f631dc406d0bd1f56ae54': 1.0
            },
            'default1': {
                '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': 4.0
            },
            'default2': {
                '2bf637d32f5d96630fd979fccadba6750537344c': 3.0,
                '01a496b0dad9b88c5a99430823877dd964e928d4': 3.0,
                '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': -8.0
            },
            'cp1': {
                '52bffbb5977ca91a7ed804e0113a8089327d125b': 1.0,
                '475a207e912aaf6c2575e8a9c7181eb7efa17396': 1.0,
                'a6a4c85bc3b6f466442537eda3a9b895dc1dd4bd': 1.0,
                'cae082641e64bff597c76887e4283ce71f598b1c': 1.0
            },
            'cp': {
                'a61b76b5591909861d3f631dc406d0bd1f56ae54': 1.0
            },
            'cp2': {
                '52bffbb5977ca91a7ed804e0113a8089327d125b': -2.0,
                'd189009ff65accb304fa062df7df876d208f0913': 1.0,
                '475a207e912aaf6c2575e8a9c7181eb7efa17396': -2.0,
                'cae082641e64bff597c76887e4283ce71f598b1c': -2.0,
                '9d0727c508b512695916d191d2e70ff287500cd8': 1.0,
                'a6a4c85bc3b6f466442537eda3a9b895dc1dd4bd': -2.0,
                'c441b65c480520f0cabc40fd02442f4c6c13ff65': 1.0,
                'e083d732a8049ac739d8cc0f8fa19964610a6819': 1.0,
                'f275f61a4bc7f9937d93d70f15d79f308001db6a': 1.0,
                '7e4b34c6a7d5d336c74bf6396cbb488e2cafb1de': 1.0
            },
            'default3': {
                '39b23b91449835038993f75242178f992e0ac0e3': 1.0,
                '2bf637d32f5d96630fd979fccadba6750537344c': -3.0,
                '345b26a6193fc8d19659d5290391f87295c11416': 3.0,
                '01a496b0dad9b88c5a99430823877dd964e928d4': -3.0,
                '2d3cf1d504374fa9050a9a28c3ab4a72df0534e7': 4.0
            }
        },
        'name': 'Ne Tetramer',
        'attributes': {}
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

    mongo = mdb.db_helper.MongoDB("127.0.0.1", 27017, "HBC6_tmp")
    db.save(mongo, name_override=True)


# Seg faults on travis
# def test_dataframe_visualization(hbc_from_df):

#     # Remap
#     db = hbc_from_df

#     # No return value to test
#     hbc_from_df.ternary()
