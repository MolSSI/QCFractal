
from mongo_qcdb import db_helper


db_helper = db_helper.db_helper
mongo = db_helper("192.168.2.139", 27017, "local")

print(mongo.get_value("ONE-ELECTRON ENERGY", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ",do_stoich=False, debug_level=1))

print(mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "B3LYP/aug-cc-pVDZ",do_stoich=False, debug_level=1))
print(mongo.get_value("ONE-ELECTRON ENERGY", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp",do_stoich=True, debug_level=1))
print(mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp", do_stoich=True, debug_level=1))
print(mongo.get_series("success", "HBC6", "cp", "WPBE/qzvp", do_stoich=True, debug_level=1))
print(mongo.get_dataframe("TWO-ELECTRON ENERGY", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", ["B3LYP/adz", "B3LYP/aug-cc-pVDZ"], do_stoich=True, debug_level=1))
