import pytest
import numpy as np
import mongo_qcdb as mdb
import math
import os
import glob
import json
from collections import OrderedDict

@pytest.fixture(scope="module")
def mongo_socket():
    db_name = "local_values_test"
    mongo = mdb.mongo_helper.MongoSocket("127.0.0.1", 27017)
    mongo.set_project(db_name)
    for db_name in mongo.client.database_names():
        mongo.client.drop_database(db_name)

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
                    inserted = mongo.add_molecule(data)
            if (col == "databases"):
                    inserted = mongo.add_database(data)
            if (col == "pages"):
                    inserted = mongo.add_page(data)

    return mongo

def test_return_value(mongo_socket):
    wpbe_val = mongo_socket.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=True, debug_level=1)
    assert wpbe_val == -0.027893331976144964

def test_return_value_fallback(mongo_socket):
    wpbe_val = mongo_socket.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp", do_stoich=True, debug_level=1)
    assert np.allclose(wpbe_val, -20.4963, rtol=1.e-3, atol=1.e-3)

def test_variable(mongo_socket):
    wpbe_val = mongo_socket.get_value("variables.SCF N ITERS", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=True, debug_level=1)
    assert wpbe_val == -8.0

def test_variable_stoichless(mongo_socket):
    wpbe_val = mongo_socket.get_value("variables.SCF N ITERS", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=False, debug_level=1)
    assert wpbe_val == [8.0, 8.0, 8.0]

def test_get_series_variable(mongo_socket):
    wpbe_val = mongo_socket.get_series("variables.SCF N ITERS", "HBC6", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=False, debug_level=1)
    assert ((wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaOO_0.9444444444444444"] == [8.0, 8.0, 8.0])
    and (wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaOO_1.3333333333333333"] == [8.0, 8.0, 8.0]))

def test_get_series_return_value(mongo_socket):
    wpbe_val = mongo_socket.get_series("return_value", "HBC6", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=True, debug_level=1)
    assert (np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaOO_1.777777777777778"], -0.001889, rtol=1.e-6, atol=1.e-6) and
    np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaNN_1.0555555555555556"], -0.033706, rtol=1.e-6, atol=1.e-6))

def test_get_dataframe_return_value(mongo_socket):
    wpbe_val = mongo_socket.get_dataframe("return_value", "HBC6", "cp", ["WPBE/qzvp", "B3LYP/aug-cc-pVDZ"], do_stoich=True, debug_level=1)
    assert np.isclose(wpbe_val["WPBE/qzvp"]["FaONFaON_1.075"], -13.0246, rtol=1.e-6, atol=1.e-6)
    assert np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaONFaON_1.075"], -0.020754, rtol=1.e-6, atol=1.e-6)

def test_get_dataframe_variable(mongo_socket):
    wpbe_val = mongo_socket.get_dataframe("variables.CURRENT DIPOLE Y", "HBC6", "cp", ["WPBE/qzvp", "B3LYP/aug-cc-pVDZ"], do_stoich=True, debug_level=1)
    assert (wpbe_val["WPBE/qzvp"]["FaONFaON_1.05"] == None)
    assert np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaONFaON_1.05"], 7.158318e-11, rtol=1.e-6, atol=1.e-6)

def test_add_remove_molecule(mongo_socket):
    data = {"_id":"NewMolecule", "symbols":"a", "masses":"b", "name":"c", "multiplicity":"d", "real":"e", "geometry":"f",
    "fragments":"e", "fragment_charges":"f", "fragment_multiplicities":"g", "provenance":"h", "comment":"i", "charge":"j"}
    result = mongo_socket.add_molecule(data)
    assert result
    result = mongo_socket.del_molecule_by_hash("6504d1e5eb2d1e0a9e979029f9a8d55fbad06fac")
    assert result
    result = mongo_socket.add_molecule(data)
    assert result
    result = mongo_socket.del_molecule_by_data(data)
    assert result

    result = mongo_socket.add_molecule(data, "some_project")
    assert result
    result = mongo_socket.del_molecule_by_hash("6504d1e5eb2d1e0a9e979029f9a8d55fbad06fac", "some_project")
    assert result
    result = mongo_socket.add_molecule(data, "some_project")
    assert result
    result = mongo_socket.del_molecule_by_data(data, "some_project")
    assert result
    mongo_socket.client.drop_database("some_project")

def test_add_remove_database(mongo_socket):
    data = {"_id":"NewDatabase", "name":"a"}
    result = mongo_socket.add_database(data)
    assert result
    result = mongo_socket.del_database_by_hash("7b3ce68b6c2f7d67dae4210eeb83be69f978e2a8")
    assert result
    result = mongo_socket.add_database(data)
    assert result
    result = mongo_socket.del_database_by_data(data)
    assert result

    result = mongo_socket.add_database(data, "some_project")
    assert result
    result = mongo_socket.del_database_by_hash("7b3ce68b6c2f7d67dae4210eeb83be69f978e2a8", "some_project")
    assert result
    result = mongo_socket.add_database(data, "some_project")
    assert result
    result = mongo_socket.del_database_by_data(data, "some_project")
    assert result
    mongo_socket.client.drop_database("some_project")

def test_add_remove_page(mongo_socket):
    data = {"_id":"NewPage", "modelchem":"a", "molecule_hash":"b"}
    result = mongo_socket.add_page(data)
    assert result
    result = mongo_socket.del_page_by_hash("b8106d3072fd101cf33f937b0db5b73e670c1dd9")
    assert result
    result = mongo_socket.add_page(data)
    assert result
    result = mongo_socket.del_page_by_data(data)
    assert result

    result = mongo_socket.add_page(data, "some_project")
    assert result
    result = mongo_socket.del_page_by_hash("b8106d3072fd101cf33f937b0db5b73e670c1dd9", "some_project")
    assert result
    result = mongo_socket.add_page(data, "some_project")
    assert result
    result = mongo_socket.del_page_by_data(data, "some_project")
    assert result
    mongo_socket.client.drop_database("some_project")

def test_batch_remove(mongo_socket):
    batch = [{"_id":"NewDatabase", "name":"a"}, {"_id":"NewDatabase2", "name":"b"}]
    for item in batch:
        assert mongo_socket.add_database(item)
    result = mongo_socket.del_database_by_hash(["7b3ce68b6c2f7d67dae4210eeb83be69f978e2a8", "205c97d9248d2cd12db1c55ba421eb8df84b22a7"])
    assert result == 2
    for item in batch:
        assert mongo_socket.add_database(item)
    result = mongo_socket.del_database_by_data(batch)
    assert result == 2

def test_list_methods(mongo_socket):
    data = {"_id":"NewPage", "modelchem":"a", "molecule_hash":"b"}
    mongo_socket.add_page(data)
    data = {"_id":"NewPage", "modelchem":"b", "molecule_hash":"b"}
    mongo_socket.add_page(data)
    data = {"_id":"NewPage", "modelchem":"c", "molecule_hash":"b"}
    mongo_socket.add_page(data)
    data = {"_id":"NewPage", "modelchem":"d", "molecule_hash":"b"}
    mongo_socket.add_page(data)
    result = mongo_socket.list_methods(["92e8b7bebf5382d5056754e62e17993ef2b1b379",
                            "8665e03700fd99259aa21c86fa2d2df11cb0dbef",
                            "f9dec31ed1e51fbb850c6d113e3b4081eebf3792",
                            "b"
                            ])
    mongo_socket.del_page_by_hash("c78d35375ca3397a1fb0db15ae559a0de839d59e")
    mongo_socket.del_page_by_hash("b8106d3072fd101cf33f937b0db5b73e670c1dd9")
    mongo_socket.del_page_by_hash("de02aa0d01a917a426ca9b37541eb2e41af1dd6b")
    mongo_socket.del_page_by_hash("be5a325825ffa06c47704d090e318088091e5cb8")
    assert list(result.as_matrix()[0])[0] == 'B3LYP/aug-cc-pVDZ'
    assert list(result.as_matrix()[1])[0] == 'B3LYP/aug-cc-pVDZ'
    assert list(result.as_matrix()[2]) == ['a', 'b', 'c', 'd']
    assert list(result.as_matrix()[3])[0] == 'B3LYP/aug-cc-pVDZ'

def test_list_methods_none(mongo_socket):
    result = mongo_socket.list_methods(["Invalid", "92e8b7bebf5382d5056754e62e17993ef2b1b379"])
    assert str(result.as_matrix()[0][0]) == "B3LYP/aug-cc-pVDZ"

def test_search_qc_return_value(mongo_socket):
    result = mongo_socket.search_qc_variable(["af4c153199d3386e8bb1ff4780df955d2adeee80", "d560eb7fe48db28eb57902e4469f2a9b8af45c71"], "return_value")
    assert np.isclose(result.as_matrix()[0][0], -150.01784074, rtol=1.e-6, atol=1.e-6)
    assert np.isclose(result.as_matrix()[1][0], -339.86866254, rtol=1.e-6, atol=1.e-6)

def test_search_qc_variable(mongo_socket):
    result = mongo_socket.search_qc_variable(["af4c153199d3386e8bb1ff4780df955d2adeee80", "d560eb7fe48db28eb57902e4469f2a9b8af45c71"], "variables.NUCLEAR REPULSION ENERGY")
    assert np.isclose(result.as_matrix()[0][0], 71.0297063, rtol=1.e-6, atol=1.e-6)
    assert np.isclose(result.as_matrix()[1][0], 221.90773477, rtol=1.e-6, atol=1.e-6)

def test_search_qc_none(mongo_socket):
    result = mongo_socket.search_qc_variable(["Nothing"], "variables.NUCLEAR REPULSION ENERGY")
    assert result.as_matrix()[0][0] == None
    result = mongo_socket.search_qc_variable(["af4c153199d3386e8bb1ff4780df955d2adeee80"], "Wrong")
    assert result.as_matrix()[0][0] == None
    result = mongo_socket.search_qc_variable(["Nothing"], "Wrong")
    assert result.as_matrix()[0][0] == None

def test_evaluate_return_value(mongo_socket):
    result = mongo_socket.evaluate(["efad0bae4a0bdf4aeea66b3c29ec505bdd61b2a1"], ["B3LYP/aug-cc-pVDZ", "Test"])
    assert np.isclose(result.as_matrix()[0][0],-189.794388, rtol=1.e-6, atol=1.e-6)
    assert math.isnan(result.as_matrix()[0][1])

def test_evaluate_exhaustive_return_value(mongo_socket):
    data = {"_id":"NewPage", "modelchem":"a", "molecule_hash":"b", "return_value":"accessed"}
    mongo_socket.add_page(data)
    result = mongo_socket.evaluate(["efad0bae4a0bdf4aeea66b3c29ec505bdd61b2a1", "b", "und"], ["B3LYP/aug-cc-pVDZ", "a", "not available"])
    mongo_socket.del_page_by_hash("b8106d3072fd101cf33f937b0db5b73e670c1dd9")
    assert math.isnan(result.as_matrix()[0][0])
    assert result.as_matrix()[0][1] == "accessed"
    assert math.isnan(result.as_matrix()[0][2])
    assert np.isclose(result.as_matrix()[1][0],-189.794388, rtol=1.e-6, atol=1.e-6)
    assert math.isnan(result.as_matrix()[1][1])
    assert math.isnan(result.as_matrix()[1][2])
    assert math.isnan(result.as_matrix()[2][0])
    assert math.isnan(result.as_matrix()[2][1])
    assert math.isnan(result.as_matrix()[2][2])

def test_evaluate_exhaustive_variables(mongo_socket):
    data = {"_id":"NewPage", "modelchem":"a", "molecule_hash":"b", "return_value":"accessed"}
    mongo_socket.add_page(data)
    result = mongo_socket.evaluate(["efad0bae4a0bdf4aeea66b3c29ec505bdd61b2a1", "b", "und"], ["B3LYP/aug-cc-pVDZ", "a", "not available"], field="variables.NUCLEAR REPULSION ENERGY")
    mongo_socket.del_page_by_hash("b8106d3072fd101cf33f937b0db5b73e670c1dd9")
    assert math.isnan(result.as_matrix()[0][0])
    assert math.isnan(result.as_matrix()[0][1])
    assert math.isnan(result.as_matrix()[0][2])
    assert np.isclose(result.as_matrix()[1][0], 69.45752938798681, rtol=1.e-6, atol=1.e-6)
    assert math.isnan(result.as_matrix()[1][1])
    assert math.isnan(result.as_matrix()[1][2])
    assert math.isnan(result.as_matrix()[2][0])
    assert math.isnan(result.as_matrix()[2][1])
    assert math.isnan(result.as_matrix()[2][2])

def test_evaluate_2_exhaustive(mongo_socket):
    data = {"_id":"NewPage", "modelchem":"B3LYP/aug-cc-pVDZ", "molecule_hash":"b", "return_value":"accessed"}
    mongo_socket.add_page(data)
    result = mongo_socket.evaluate_2(["efad0bae4a0bdf4aeea66b3c29ec505bdd61b2a1", "6f34560054454808dbd49c407de31f08b58dcbbe", "b", "und"], ["return_value", "variables.NUCLEAR REPULSION ENERGY", "invalid"], "B3LYP/aug-cc-pVDZ")
    mongo_socket.del_page_by_hash("b8106d3072fd101cf33f937b0db5b73e670c1dd9")
    assert np.isclose(result.as_matrix()[0][0], -189.79511532157892, rtol=1.e-6, atol=1.e-6)
    assert np.isclose(result.as_matrix()[0][1], 69.42994763149548, rtol=1.e-6, atol=1.e-6)
    assert math.isnan(result.as_matrix()[0][2])
    assert result.as_matrix()[1][0] == "accessed"
    assert math.isnan(result.as_matrix()[1][1])
    assert math.isnan(result.as_matrix()[1][2])
    assert np.isclose(result.as_matrix()[2][0], -189.79438817699523, rtol=1.e-6, atol=1.e-6)
    assert np.isclose(result.as_matrix()[2][1], 69.45752938798681, rtol=1.e-6, atol=1.e-6)
    assert math.isnan(result.as_matrix()[2][2])
    assert math.isnan(result.as_matrix()[3][0])
    assert math.isnan(result.as_matrix()[3][1])
    assert math.isnan(result.as_matrix()[3][2])

def test_project_list(mongo_socket):
    res = mongo_socket.list_projects()
    assert len(res) == 1
    assert res[0] == "local_values_test"

def test_clone(mongo_socket):
    res = mongo_socket.list_projects()
    assert len(res) == 1
    assert res[0] == "local_values_test"
    mongo_socket.client["clone"]["dummy"].insert_one({"test":"record"})
    assert len(mongo_socket.list_projects()) == 2
    assert "dummy" in mongo_socket.client["clone"].collection_names()
    mongo_socket.clone_to("127.0.0.1", 27017, "clone")
    assert len(mongo_socket.list_projects()) == 2
    assert "dummy" not in mongo_socket.client["clone"].collection_names()
    mongo_socket.client.drop_database("clone")
    assert len(mongo_socket.list_projects()) == 1

def test_push(mongo_socket):
    res = mongo_socket.list_projects()
    assert len(res) == 1
    assert res[0] == "local_values_test"
    mongo_socket.client["push"]["dummy"].insert_one({"test":"record"})
    assert len(mongo_socket.list_projects()) == 2
    assert "dummy" in mongo_socket.client["push"].collection_names()
    mongo_socket.push_to("127.0.0.1", 27017, "push")
    assert len(mongo_socket.list_projects()) == 2
    assert "dummy" in mongo_socket.client["push"].collection_names()
    mongo_socket.client.drop_database("push")
    assert len(res) == 1
