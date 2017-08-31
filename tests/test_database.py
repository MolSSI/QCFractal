import numpy as np
import pandas as pd
from collections import OrderedDict
import glob
import sys
import json
import os

import datenqm as dqm
from datenqm import test_util
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
        # print(k)
        _compare_stoichs(stoich[k], stoich_other[k])

    return True


# Build a interesting database
@pytest.fixture
def water_db():
    db = dqm.Database("Water Data")

    # Build the water dimer.
    dimer = dqm.Molecule(_water_dimer_minima)
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
        "Water Dimer, dimer - str (invalid)", [(_water_dimer_minima, 1.0),
                                               (_water_dimer_minima.splitlines()[-1], 0.0)],
        attributes={"R": "Minima"},
        reaction_results={"Benchmark": -20.0,
                          "DFT": -10.0})

    # Add single stoich rxn via hashes
    db.add_rxn(
        "Water Dimer, nocp - hash", [(dimer.get_hash(), 1.0), (frag_0.get_hash(), -1.0),
                                     (frag_1.get_hash(), -1.0)],
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
    db = dqm.Database("N-Body Data")

    dimer = dqm.Molecule(_water_dimer_minima, name="Water Dimer")
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

    # Ne Tetramer benchmark
    db.ne_stoich = {
        'name': 'Ne Tetramer',
        'stoichiometry': {
            'default1': {
                'acfce0c62cac57640d221a545998711853606bc9': 4.0
            },
            'cp1': {
                '532ae1b0fab346aeaa2167972b5b61079081209f': 1.0,
                'f89a033c52c527b0a85f271193f3515b0aec190b': 1.0,
                '43fafa29d4d3197738e741051aff71ffe98264b3': 1.0,
                'cf36366bf4d5a2f96cf0669425c49bd929d7f081': 1.0
            },
            'default2': {
                'acfce0c62cac57640d221a545998711853606bc9': -8.0,
                'e643e8f4e8668793165339e98f4f314cdac6e909': 1.0,
                '0768204c7ddc575a77574b49598b89eb04d4522b': 1.0,
                '05cb2410011128a45b5c3479e7724c60b91bfbee': 2.0,
                '444dba64349a093f8da2c5c6594b128f54619d92': 1.0,
                '75608da1aad0be8f353cab54e2674a138c5d04e7': 1.0
            },
            'cp2': {
                '532ae1b0fab346aeaa2167972b5b61079081209f': -2.0,
                'f89a033c52c527b0a85f271193f3515b0aec190b': -2.0,
                '43fafa29d4d3197738e741051aff71ffe98264b3': -2.0,
                'cf36366bf4d5a2f96cf0669425c49bd929d7f081': -2.0,
                'f08a3ea572f4a371c18b99be63545a23f9cf17d2': 1.0,
                'eb6efb5f6a7238c0582517eeba0c5b940d84c51d': 1.0,
                '75a23e5e6f995aaee9a049226701f543f0ee9a82': 1.0,
                '8d5280d9aa82243a12b319b24bd19506bae77853': 1.0,
                'b7f73f0957f1f14469dde2000605061fbbe4939e': 1.0,
                'bee9dfe443c016283ef2a4d98959a736b0d0b591': 1.0
            },
            'default3': {
                'acfce0c62cac57640d221a545998711853606bc9': 4.0,
                'e643e8f4e8668793165339e98f4f314cdac6e909': -1.0,
                '0768204c7ddc575a77574b49598b89eb04d4522b': -1.0,
                '05cb2410011128a45b5c3479e7724c60b91bfbee': -2.0,
                '444dba64349a093f8da2c5c6594b128f54619d92': -1.0,
                '75608da1aad0be8f353cab54e2674a138c5d04e7': -1.0,
                'da4d287f648d33203652cd5a8fc621e331c16fab': 1.0,
                'd7914b7213eff227af8228f64c26187eec343c21': 1.0,
                'efcfd9e9ae9f33dcfb2780367690dcfff8bcb397': 1.0,
                '4e3e461ab1439dc360a4b001ac885a8240b27640': 1.0
            },
            'cp3': {
                '532ae1b0fab346aeaa2167972b5b61079081209f': 1.0,
                'f89a033c52c527b0a85f271193f3515b0aec190b': 1.0,
                '43fafa29d4d3197738e741051aff71ffe98264b3': 1.0,
                'cf36366bf4d5a2f96cf0669425c49bd929d7f081': 1.0,
                'f08a3ea572f4a371c18b99be63545a23f9cf17d2': -1.0,
                'eb6efb5f6a7238c0582517eeba0c5b940d84c51d': -1.0,
                '75a23e5e6f995aaee9a049226701f543f0ee9a82': -1.0,
                '8d5280d9aa82243a12b319b24bd19506bae77853': -1.0,
                'b7f73f0957f1f14469dde2000605061fbbe4939e': -1.0,
                'bee9dfe443c016283ef2a4d98959a736b0d0b591': -1.0,
                '32b290f24926d5ce6ca7ed4eb31d034bcc18a784': 1.0,
                '214a0c4469f1ea514e0ee7a256dc601f39a90498': 1.0,
                'fb3eeb93515fac8bb807af999bddd883c27af7fa': 1.0,
                '3e4f560822d53d4fd53520d354eb4fa6632c745a': 1.0
            },
            'default': {
                'c0e6a8225f1eb3ab5f7e81a755a91dd800869f23': 1.0
            },
            'cp': {
                'c0e6a8225f1eb3ab5f7e81a755a91dd800869f23': 1.0
            }
        },
        'attributes': {},
        'reaction_results': {
            'default': {}
        }
    }
    return db


# Build HBC from dataframe
@pytest.fixture(scope="module")
def hbc_from_df():

    fn = os.path.abspath(os.path.dirname(__file__)) + "/../databases/DB_HBC6/HBC6.pd_pkl"
    df = pd.read_pickle(fn)

    db = dqm.Database("HBC_6", db_type="ie")

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
    ms = dqm.mongo_helper.MongoSocket("127.0.0.1", 27017, db_name)
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

    bench_vals = {
        "default1": 1,
        "cp1": 4,
        "default2": 6,
        "cp2": 10,
        "default3": 10,
        "cp3": 14,
        "default": 1,
        "cp": 1
    }
    # Check some basics
    for key in list(nbody_db.ne_stoich["stoichiometry"]):
        assert bench_vals[key] == len(nbody_db.ne_stoich["stoichiometry"][key])

    # Check the N-body
    ne_stoich = nbody_db.get_rxn("Ne Tetramer")
    mh = list(ne_stoich["stoichiometry"]["default"])[0]
    # print(ne_stoich)
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
        -0.263942, db.statistics("ME", "B3LYP/aug-cc-pVDZ", bench="B3LYP/def2-QZVP"), atol=1.e-5)

    # Different benchmark
    assert np.allclose(
        -0.263942,
        db.statistics("ME", db["B3LYP/aug-cc-pVDZ"], bench=db["B3LYP/def2-QZVP"]),
        atol=1.e-5)
    assert np.allclose(
        -0.263942,
        db.statistics("ME", db["B3LYP/aug-cc-pVDZ"], bench=np.asarray(db["B3LYP/def2-QZVP"])),
        atol=1.e-5)


def test_dataframe_saving_loading(hbc_from_df):

    # Remap
    db = hbc_from_df

    tmp_db_name = "local_test_save_load"
    mongo = dqm.mongo_helper.MongoSocket("127.0.0.1", 27017, tmp_db_name)

    # Dangerous, probably do not want a function that does this
    if tmp_db_name in mongo.client.database_names():
        mongo.client.drop_database(tmp_db_name)

    db.save(mongo)
    db_from_save = dqm.Database(db.data["name"], socket=mongo)

    assert db_from_save.rxn_index.shape[0] == 588


def test_query(mongo_socket):

    db = dqm.Database("HBC6", socket=mongo_socket)
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
