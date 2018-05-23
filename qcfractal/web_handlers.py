
from tornado.options import options, define
import tornado.ioloop
import tornado.web
import pymongo
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

        #print(self.request.headers["Content-Type"])
        self.json = json.loads(self.request.body.decode("UTF-8"))

        # Set logging
        # print(self.request.method)
        # self.objects["logger"].info("%s" % __api_name__)


class Molecule(APIHandler):
    """
    A handler to push and get molecules.
    """

    # __api_name__ = "Molecule"
    def get(self):

        db = self.objects["db_socket"]

        kwargs = {}
        if "index" in self.json["data"]:
            kwargs["index"] = self.json["data"]["index"]

        ret = {}
        ret["data"] = db.get_molecules(self.json["data"]["ids"], **kwargs)

        self.write(ret)

    def post(self):

        db = self.objects["db_socket"]

        ret = db.add_molecules(self.json["data"]["molecules"])
        self.write(ret)


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


