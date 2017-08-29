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
        print(k)
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

    # Diagnositics
    # key1 = list(db.get_rxn("Water Dimer, bench")["stoichiometry"]["default"])[0]
    # key2 = list(db.get_rxn("Water Dimer")["stoichiometry"]["default"])[0]
    # print(db._new_molecule_jsons[key1])
    # print(db._new_molecule_jsons[key2])
    # print(dqm.fields.get_hash(db._new_molecule_jsons[key1], "molecule"))
    # print(dqm.fields.get_hash(db._new_molecule_jsons[key2], "molecule"))

    # Nothing to fragment
    # with pytest.raises(AttributeError):
    #     db.add_ie_rxn("Water MonomerA", frag_0)

    # Ne Tetramer benchmark
    db.ne_stoich = {'stoichiometry': {'cp1': {'a88578f05682d3c5791e482fb37dea11d1e8813f': 1.0, 'c6a3f4a26b0273fb147ad36f976b064860e032ed': 1.0, '0df2386d614a378f984dac6c15ef8aa4e8d60daa': 1.0, '29572c3a40b94aeea066f67e8c104a9a992bb4fd': 1.0}, 'cp3': {'a88578f05682d3c5791e482fb37dea11d1e8813f': 1.0, 'c6a3f4a26b0273fb147ad36f976b064860e032ed': 1.0, '60aeab8c65af2361809dff8ecc1c26978ee10e97': 1.0, '9a3bc5cd98988fb306cb500f35823f438eb2dab6': -1.0, '3d3dbb632fbe1427ce26ae1664b0d3c0f74d5b12': -1.0, 'c81f24a85816ce42f67f641cf071aa1f6728c1fe': 1.0, '10cb76fc37fdd7824ad88110652f17dec0454228': -1.0, '0df2386d614a378f984dac6c15ef8aa4e8d60daa': 1.0, '608f2212cb1d976f8985ed8063ebe7a3a137997f': 1.0, '29572c3a40b94aeea066f67e8c104a9a992bb4fd': 1.0, '9ce1d375ad154bffc0448c9177395bcf191ea517': -1.0, 'c2cb626a1eb42c3dae0a9176a746ef32ac7a0864': 1.0, 'dc25c79a239743d2d885e419709bcf25f7d259cd': -1.0, 'c1ca595e29dfa9b1c071c77587a242979eea5a86': -1.0}, 'default': {'3f8425826b76f94a5e4208366f04fb28c393baa7': 1.0}, 'default2': {'b67bfd4d68aed0602f0ee439cfafe242ac97fee6': 1.0, '3e80fa18b77b2ae32e93a9e81175476dcd9cd1de': 1.0, '0eaa463b14924d7138ac330ade3169376493c457': -8.0, 'f5306d9eff58b6101e1cc6d9af271a21f01e9309': 1.0, '9850fee49a3105b99390ccc2548fd0ccaf733adb': 2.0, '2c5c9a90eb9eb665bd09e8ed04a5eae6015a0a55': 1.0}, 'cp2': {'10cb76fc37fdd7824ad88110652f17dec0454228': 1.0, 'a88578f05682d3c5791e482fb37dea11d1e8813f': -2.0, 'c6a3f4a26b0273fb147ad36f976b064860e032ed': -2.0, '0df2386d614a378f984dac6c15ef8aa4e8d60daa': -2.0, '29572c3a40b94aeea066f67e8c104a9a992bb4fd': -2.0, '9ce1d375ad154bffc0448c9177395bcf191ea517': 1.0, '9a3bc5cd98988fb306cb500f35823f438eb2dab6': 1.0, '3d3dbb632fbe1427ce26ae1664b0d3c0f74d5b12': 1.0, 'dc25c79a239743d2d885e419709bcf25f7d259cd': 1.0, 'c1ca595e29dfa9b1c071c77587a242979eea5a86': 1.0}, 'default3': {'b67bfd4d68aed0602f0ee439cfafe242ac97fee6': -1.0, '6a013d7f915c2ab2e94939fc425d509a984c5a82': 1.0, '3e80fa18b77b2ae32e93a9e81175476dcd9cd1de': -1.0, '10082952153bec30467290841283d8c4ca981d00': 1.0, '0eaa463b14924d7138ac330ade3169376493c457': 4.0, 'f5306d9eff58b6101e1cc6d9af271a21f01e9309': -1.0, '9850fee49a3105b99390ccc2548fd0ccaf733adb': -2.0, 'ebc7d371336efbe9e651357ade6c9bd6fdb78739': 1.0, '095be43cff2cd62f882fa99e65a66451829fd9e0': 1.0, '2c5c9a90eb9eb665bd09e8ed04a5eae6015a0a55': -1.0}, 'default1': {'0eaa463b14924d7138ac330ade3169376493c457': 4.0}, 'cp': {'3f8425826b76f94a5e4208366f04fb28c393baa7': 1.0}}, 'attributes': {}, 'name': 'Ne Tetramer', 'reaction_results': {'default': {}}}
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
