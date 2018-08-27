"""
Web handlers for the FractalServer
"""
import json
import tornado.web
import functools
from base64 import b64decode, b64encode


def authenticate(permissions):
    def decorator(function):
        def wrapper(*args, **kwargs):
            handler = args[0]

            result = function(*args, **kwargs)
            return result

        return wrapper

    return decorator


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

        if "Authorization" in self.request.headers:
            split = self.request.headers["Authorization"].strip().split(' ')
            self.username, self.password = b64decode(split[1]).decode().split(':', 1)
        else:
            self.username = None
            self.password = None

    def authenticate(self, permission):
        """Authenticates request with a given permission setting

        Parameters
        ----------
        permission : str
            The required permission ["read", "write", "compute", "admin"]

        """
        verified, msg = self.objects["db_socket"].verify_user(self.username, self.password, permission)
        if verified is False:
            raise tornado.web.HTTPError(status_code=401, reason=msg)


class MoleculeHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):
        """

        Experimental documentation, need to find a decent format.

        Request:
            "meta" - Overall options to the Molecule pull request
                - "index" - What kind of index used to find the data ("id", "molecule_hash")
            "data" - A dictionary of {key : index} requests

        Returns:
            "meta" - Metadata associated with the query
                - "errors" - A list of errors in (index, error_id) format.
                - "n_found" - The number of molecule found.
                - "success" - If the query was successful or not.
                - "error_description" - A string based description of the error or False
                - "missing" - A list of keys that were not found.
            "data" - A dictionary of {key : molecule JSON} results

        """
        self.authenticate("read")

        db = self.objects["db_socket"]

        kwargs = {}
        if "index" in self.json["meta"]:
            kwargs["index"] = self.json["meta"]["index"]

        ret = db.get_molecules(self.json["data"], **kwargs)
        self.logger.info("GET: Molecule - {} pulls.".format(len(ret["data"])))

        self.write(ret)

    def post(self):
        """
            Experimental documentation, need to find a decent format.

        Request:
            "meta" - Overall options to the Molecule pull request
                - No current options
            "data" - A dictionary of {key : molecule JSON} requests

        Returns:
            "meta" - Metadata associated with the query
                - "errors" - A list of errors in (index, error_id) format.
                - "n_inserted" - The number of molecule inserted.
                - "success" - If the query was successful or not.
                - "error_description" - A string based description of the error or False
                - "duplicates" - A list of keys that were already inserted.
            "data" - A dictionary of {key : id} results
        """

        self.authenticate("write")

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
