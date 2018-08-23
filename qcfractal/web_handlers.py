
import tornado.ioloop
import tornado.web
import json

class APIHandler(tornado.web.RequestHandler):
    """
    A requests handler for API calls, build
    """

    def initialize(self, **objects):
        """
        Initializes the request to JSON, adds objects, and logging.
        """

        self.set_header("Content-Type", "application/json")
        self.objects = objects
        self.logger = objects["logger"]

        #print(self.request.headers["Content-Type"])
        self.json = json.loads(self.request.body.decode("UTF-8"))
        # Set logging
        # print(self.request.method)
        # self.objects["logger"].info("%s" % __api_name__)


class MoleculeHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    # __api_name__ = "Molecule"
    def get(self):

        db = self.objects["db_socket"]

        kwargs = {}
        if "index" in self.json["meta"]:
            kwargs["index"] = self.json["meta"]["index"]

        ret = db.get_molecules(self.json["data"], **kwargs)
        self.logger.info("GET: Molecule - {} pulls.".format(len(ret["data"])))

        self.write(ret)

    def post(self):

        db = self.objects["db_socket"]

        ret = db.add_molecules(self.json["data"])
        self.logger.info("POST: Molecule - {} inserted.".format(ret["meta"]["n_inserted"]))
        self.write(ret)

class OptionHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):

        db = self.objects["db_socket"]

        ret = db.get_options(self.json["data"])
        self.logger.info("GET: Options - {} pulls.".format(len(ret["data"])))

        self.write(ret)

    def post(self):

        db = self.objects["db_socket"]

        ret = db.add_options(self.json["data"])
        self.logger.info("POST: Options - {} inserted.".format(ret["meta"]["n_inserted"]))

        self.write(ret)

class DatabaseHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):

        db = self.objects["db_socket"]

        ret = db.get_databases(self.json["data"])
        self.logger.info("GET: Databases - {} pulls.".format(len(ret["data"])))

        self.write(ret)

    def post(self):

        db = self.objects["db_socket"]

        ret = db.add_database(self.json["data"])
        self.logger.info("POST: Databases - {} inserted.".format(ret["meta"]["n_inserted"]))

        self.write(ret)

class ResultHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):

        db = self.objects["db_socket"]
        proj = None
        if "projection" in self.json["meta"]:
            proj = self.json["meta"]["projection"]

        ret = db.get_results(self.json["data"], projection=proj)
        self.logger.info("GET: Results - {} pulls.".format(len(ret["data"])))

        self.write(ret)

    def post(self):

        db = self.objects["db_socket"]

        ret = db.add_results(self.json["data"])
        self.logger.info("POST: Results - {} inserted.".format(ret["meta"]["n_inserted"]))

        self.write(ret)

class ServiceHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):

        db = self.objects["db_socket"]

        ret = db.get_services(self.json["data"], by_id=True)
        self.logger.info("GET: Services - {} pulls.".format(len(ret["data"])))

        self.write(ret)

    # def post(self):

    #     db = self.objects["db_socket"]

    #     ret = db.add_results(self.json["data"])
    #     self.logger.info("POST: Results - {} inserted.".format(ret["meta"]["n_inserted"]))

    #     self.write(ret)



# def _check_auth(objects, header):
#     auth = False
#     try:
#         objects["mongod_socket"].client.database_names()
#         username = "default"
#         auth = True
#     except pymongo.errors.OperationFailure:

#         # The authenticate method should match a username and password
#         # to a username and password hash in the database users table.
#         db = self.objects["mongod_socket"][header["project"]]
#         try:
#             auth = db.authenticate(header["username"], header["password"])
#         except pymongo.errors.OperationFailure:
#             auth = False

#     if auth is not True:
#         raise KeyError("Could not authenticate user.")


# class Information(tornado.web.RequestHandler):
#     """
#     Obtains generic information about the Application Objects
#     """

#     def initialize(self, **objects):
#         self.objects = objects

#         if "logger" in list(self.objects):
#             self.logger = self.objects["logger"]
#         else:
#             self.logger = logging.getLogger('Information')
#         self.logger.info("INFO: %s" % self.request.method)

#     def get(self):
#         _check_auth(self.objects, self.request.headers)

#         queue = self.objects["queue_socket"]
#         mongod = self.objects["mongod_socket"]

#         ret = {}
#         ret["mongo_data"] = (mongod.url, mongod.port)
#         ret["dask_data"] = str(queue.host) + ":" + str(queue.port)
#         self.write(json.dumps(ret))


