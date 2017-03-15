import numpy as np
import pandas as pd
from collections import OrderedDict
import glob
import sys
import json
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
        reaction_results={"Benchmark": -20.0,
                          "DFT": -10.0})

    # Add single stoich from strings, not a valid set
    db.add_rxn(
        "Water Dimer, dimer - str (invalid)",
        [(_water_dimer_minima, 1.0), (_water_dimer_minima.splitlines()[-1], 0.0)],
        attributes={"R": "Minima"},
        reaction_results={"Benchmark": -20.0,
                          "DFT": -10.0})

    # Add single stoich rxn via hashes
    db.add_rxn(
        "Water Dimer, nocp - hash",
        [(dimer.get_hash(), 1.0), (frag_0.get_hash(), -1.0), (frag_1.get_hash(), -1.0)],
        attributes={"R": "Minima"},
        reaction_results={"Benchmark": -5.0})

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
        'name': 'Ne Tetramer',
        'reaction_results': {
            'default': {}
        },
        'stoichiometry': {
            'cp': {
                '41806e2d95255309b2b7f25149e073a37282e50b': 1.0
            },
            'default2': {
                '5c295cf735e5200ef69f521c738fbfa1ad4fc8d6': 1.0,
                '7d41a0004140b70ae29eb6dc9b729078848eda31': -8.0,
                'ed43fa123f66c945660bcb1e0f158fcf71ff9089': 1.0,
                '949f9082c959c3cb316fdb534002d47e84542a6d': 1.0,
                '1360f433f6dd339e16da750d0a21f151242b6900': 2.0,
                '57590ae0cdfaff83ea360d1c418787e9d4bbf5e4': 1.0
            },
            'cp2': {
                '5fc6400a4a28657c4643f956ebe4f4e3a68aecfc': 1.0,
                '115f82f0056f9540cc234e19ef1f81e313cc9a30': 1.0,
                '064ecf3401e08c5055241adb212faedbf4d2d7e5': -2.0,
                'b3ebb6bc5adba02f7e88abf7ff4a85060335fba3': -2.0,
                'ae59c9f1e827e78d745fa925e88dd2d7aa80fb6d': -2.0,
                '6af5906a998bc2e85aaaeefee2ad1fa5b682a460': 1.0,
                '3242832baa23a02d0494eb81178bab89ef8a82bf': 1.0,
                'e7b148ceca9e4128bb75d33e6269c9373aedac50': 1.0,
                '63ff4c9fbfd7f57ade8ba0a3909cd50e4d3feb2d': -2.0,
                '74ed8dfb65e067805c2c31464569335558e4401e': 1.0
            },
            'cp1': {
                'b3ebb6bc5adba02f7e88abf7ff4a85060335fba3': 1.0,
                '064ecf3401e08c5055241adb212faedbf4d2d7e5': 1.0,
                '63ff4c9fbfd7f57ade8ba0a3909cd50e4d3feb2d': 1.0,
                'ae59c9f1e827e78d745fa925e88dd2d7aa80fb6d': 1.0
            },
            'cp3': {
                '89d622be9221f7b2e2a921cd91de8a4f12d2bdbb': 1.0,
                'b3ebb6bc5adba02f7e88abf7ff4a85060335fba3': 1.0,
                'e2027c85c865f232e618a2c3f191591d6f44f8bb': 1.0,
                '3242832baa23a02d0494eb81178bab89ef8a82bf': -1.0,
                '145590a213ac1a8c25904443b8e69bc280f65995': 1.0,
                '6af5906a998bc2e85aaaeefee2ad1fa5b682a460': -1.0,
                '63ff4c9fbfd7f57ade8ba0a3909cd50e4d3feb2d': 1.0,
                '115f82f0056f9540cc234e19ef1f81e313cc9a30': -1.0,
                '064ecf3401e08c5055241adb212faedbf4d2d7e5': 1.0,
                '06048a1bb6642e10b50240b5ecfaed69b3183822': 1.0,
                'ae59c9f1e827e78d745fa925e88dd2d7aa80fb6d': 1.0,
                'e7b148ceca9e4128bb75d33e6269c9373aedac50': -1.0,
                '5fc6400a4a28657c4643f956ebe4f4e3a68aecfc': -1.0,
                '74ed8dfb65e067805c2c31464569335558e4401e': -1.0
            },
            'default': {
                '41806e2d95255309b2b7f25149e073a37282e50b': 1.0
            },
            'default1': {
                '7d41a0004140b70ae29eb6dc9b729078848eda31': 4.0
            },
            'default3': {
                '5c295cf735e5200ef69f521c738fbfa1ad4fc8d6': -1.0,
                '74ad74e6388872a630f8fa3f9fa2e9ef86537742': 1.0,
                '7d41a0004140b70ae29eb6dc9b729078848eda31': 4.0,
                'ed43fa123f66c945660bcb1e0f158fcf71ff9089': -1.0,
                '949f9082c959c3cb316fdb534002d47e84542a6d': -1.0,
                '74ade2c691b8da35b4974ebfe12ae37af239a471': 1.0,
                '16f0c5eeda0ed9466157d1728873b5538e759b12': 1.0,
                '854379dd6bd4eb03bbe1f25f93a3ce91e92e8e81': 1.0,
                '1360f433f6dd339e16da750d0a21f151242b6900': -2.0,
                '57590ae0cdfaff83ea360d1c418787e9d4bbf5e4': -1.0
            }
        }
    }

    return db


# Build HBC from dataframe
@pytest.fixture(scope="module")
def hbc_from_df():

    fn = os.path.abspath(os.path.dirname(__file__)) + "/../databases/DB_HBC6/HBC6.pd_pkl"
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
            reaction_results=rvals)

    return db


@pytest.fixture(scope="module")
def mongo_socket():
    db_name = "local_values_test"
    ms = mdb.mongo_helper.MongoSocket("127.0.0.1", 27017, db_name)
    for db_name in ms.client.database_names():
        ms.client.drop_database(db_name)

    collections = ["molecules", "databases", "pages"]

    # Define the descriptor field for each collection. Used for logging.
    descriptor = {"molecules": "name", "databases": "name", "pages": "modelchem"}

    # Add all JSON
    for col in collections:
        prefix = os.path.dirname(os.path.abspath(__file__)) + "/../databases/DB_HBC6/" + col + "/"
        for filename in glob.glob(prefix + "*.json"):
            json_data = open(filename).read()
            # Load JSON from file into OrderedDict
            data = json.loads(json_data, object_pairs_hook=OrderedDict)
            if (col == "molecules"):
                inserted = ms.add_molecule(data)
            if (col == "databases"):
                inserted = ms.add_database(data)
            if (col == "pages"):
                inserted = ms.add_page(data)

    return ms


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
    print(ne_stoich)
    _compare_rxn_stoichs(nbody_db.ne_stoich, ne_stoich)


# Test dataframe
def test_dataframe_reaction_results(water_db):

    water_db.query("Benchmark", reaction_results=True, scale=1.0)
    water_db.query("DFT", reaction_results=True, scale=1)

    assert water_db.df.ix["Water Dimer, nocp", "Benchmark"] == -20.0
    assert water_db.df.ix["Water Dimer, nocp", "DFT"] == -10.0
    assert water_db.df.ix["Water Dimer, nocp - hash", "Benchmark"] == -5.0

    assert np.isnan(water_db.df.ix["Water Dimer, nocp - hash", "DFT"])


def test_dataframe_stats(hbc_from_df):

    # Remap
    db = hbc_from_df

    db.query("Benchmark", reaction_results=True, scale=1.0)
    db.query("B3LYP/aug-cc-pVDZ", reaction_results=True, scale=1.0)
    db.query("B3LYP/def2-QZVP", reaction_results=True, scale=1.0)

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

    tmp_db_name = "local_test_save_load"
    mongo = mdb.mongo_helper.MongoSocket("127.0.0.1", 27017, tmp_db_name)

    # Dangerous, probably do not want a function that does this
    if tmp_db_name in mongo.client.database_names():
        mongo.client.drop_database(tmp_db_name)

    db.save(mongo)
    db_from_save = mdb.Database(db.data["name"], socket=mongo)

    assert db_from_save.rxn_index.shape[0] == 588


def test_query(mongo_socket):

    db = mdb.Database("HBC6", socket=mongo_socket)
    db.query("B3LYP/aug-cc-pVDZ", stoich="cp", prefix="cp-")
    db.query("B3LYP/adz", stoich="cp", reaction_results=True, scale=1.0)

    mue = db.statistics("MUE", "cp-B3LYP/aug-cc-pVDZ", bench="B3LYP/adz")
    assert np.allclose(0.0, mue, atol=1.e-4)

    # Shouldnt do anything
    db.refresh()
    mue = db.statistics("MUE", "cp-B3LYP/aug-cc-pVDZ", bench="B3LYP/adz")
    assert np.allclose(0.0, mue, atol=1.e-4)


# Seg faults on travis
# def test_dataframe_visualization(hbc_from_df):

#     # Remap
#     db = hbc_from_df

#     # No return value to test
#     hbc_from_df.ternary()
