
from mongo_qcdb import db_helper


db_helper = db_helper.db_helper
mongo = db_helper("192.168.2.139", 27017, "local")

print(mongo.get_value("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp", debug_level=1))
print(mongo.get_series("return_value", "HBC6", "cp", "WPBE/qzvp", debug_level=1))
print(mongo.get_dataframe("return_value", "HBC6", "FaOOFaOO_0.9444444444444444", "cp", ["B3LYP/adz", "B3LYP/aug-cc-pVDZ"], debug_level=1))
