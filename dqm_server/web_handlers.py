
from tornado.options import options, define
import tornado.ioloop
import tornado.web
import pymongo

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

        # Set logging
        self.objects["logger"].info("INFO: %s" % self.request.method)


class Molecule(APIHandler):

    def get(self):

        query = self.request.body
        print(query)
        # db = self.objects["db"]


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


