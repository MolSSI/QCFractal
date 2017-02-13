import pytest
from mongo_qcdb.db_helper import db_helper
import numpy as np


mongo = db_helper("127.0.0.1", 27017, "local")

#print(mongo.get_value("ONE-ELECTRON ENERGY", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ",do_stoich=False, debug_level=1))
#
#print(mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ",do_stoich=False, debug_level=1))
#print(mongo.get_value("ONE-ELECTRON ENERGY", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp",do_stoich=True, debug_level=1))
#print(mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp", do_stoich=True, debug_level=1))
#print(mongo.get_series("success", "HBC6", "cp", "WPBE/qzvp", do_stoich=True, debug_level=1))
#print(mongo.get_dataframe("TWO-ELECTRON ENERGY", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", ["B3LYP/adz", "B3LYP/aug-cc-pVDZ"], do_stoich=True, debug_level=1))


#def test_return_values():
#    wpbe_val = mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp", do_stoich=True, debug_level=1)
#    assert np.allclose(wpbe_val, -20.4963, rtol=1.e-3, atol=1.e-3) 
