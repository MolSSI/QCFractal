
from mongo_qcdb import db_helper


db_helper = db_helper.db_helper
mongo = db_helper("192.168.2.139", 27017, "local")

print(mongo.get_value("HBC6", "FaOOFaOO_0.9444444444444444", "cp", "WPBE/qzvp"))
print(mongo.get_series("HBC6", "cp", "WPBE/qzvp"))
print(mongo.get_dataframe("HBC6", "FaOOFaOO_0.9444444444444444", "cp", ["B3LYP/adz", "B3LYP/aug-cc-pVDZ"]))
