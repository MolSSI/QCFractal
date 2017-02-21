import mongo_qcdb as mdb

mongo = mdb.mongo_socket.MongoDB("127.0.0.1", 27017, "local")

wpbe_val = mongo.get_dataframe("variables.CURRENT DIPOLE Y", "HBC6", "cp", ["WPBE/qzvp", "B3LYP/aug-cc-pVDZ"], do_stoich=True, debug_level=1)
print(wpbe_val)
