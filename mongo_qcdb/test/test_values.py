import pytest
import numpy as np
import mongo_qcdb as mdb


mongo = mdb.db_helper.MongoSocket("127.0.0.1", 27017, "local")

def test_return_value():
    wpbe_val = mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=True, debug_level=1)
    assert wpbe_val == -0.027893331976144964

def test_return_value_fallback():
    wpbe_val = mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp", do_stoich=True, debug_level=1)
    assert np.allclose(wpbe_val, -20.4963, rtol=1.e-3, atol=1.e-3)

def test_variable():
    wpbe_val = mongo.get_value("variables.SCF N ITERS", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=True, debug_level=1)
    assert wpbe_val == -8.0

def test_variable_stoichless():
    wpbe_val = mongo.get_value("variables.SCF N ITERS", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=False, debug_level=1)
    assert wpbe_val == [8.0, 8.0, 8.0]

def test_get_series_variable():
    wpbe_val = mongo.get_series("variables.SCF N ITERS", "HBC6", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=False, debug_level=1)
    assert ((wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaOO_0.9444444444444444"] == [8.0, 8.0, 8.0])
    and (wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaOO_1.3333333333333333"] == [8.0, 8.0, 8.0]))

def test_get_series_return_value():
    wpbe_val = mongo.get_series("return_value", "HBC6", "cp", "B3LYP/aug-cc-pVDZ", do_stoich=True, debug_level=1)
    assert (np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaOO_1.777777777777778"], -0.001889, rtol=1.e-6, atol=1.e-6) and
    np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaOOFaNN_1.0555555555555556"], -0.033706, rtol=1.e-6, atol=1.e-6))

def test_get_dataframe_return_value():
    wpbe_val = mongo.get_dataframe("return_value", "HBC6", "cp", ["WPBE/qzvp", "B3LYP/aug-cc-pVDZ"], do_stoich=True, debug_level=1)
    assert (np.isclose(wpbe_val["WPBE/qzvp"]["FaONFaON_1.075"], -13.0246, rtol=1.e-6, atol=1.e-6) and
    np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaONFaON_1.075"], -0.020754, rtol=1.e-6, atol=1.e-6))

def test_get_dataframe_variable():
    wpbe_val = mongo.get_dataframe("variables.CURRENT DIPOLE Y", "HBC6", "cp", ["WPBE/qzvp", "B3LYP/aug-cc-pVDZ"], do_stoich=True, debug_level=1)
    assert ((wpbe_val["WPBE/qzvp"]["FaONFaON_1.05"] == None) and
    (np.isclose(wpbe_val["B3LYP/aug-cc-pVDZ"]["FaONFaON_1.05"], 7.158318e-11, rtol=1.e-6, atol=1.e-6)))
