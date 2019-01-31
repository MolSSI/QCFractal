"""
Web handlers for the FractalServer
"""
import json

import tornado.web

from .interface.models.rest_models import (
    MoleculeGETBody, MoleculeGETResponse, MoleculePOSTBody, MoleculePOSTResponse,
    OptionGETBody, OptionGETResponse, OptionPOSTBody, OptionPOSTResponse,
    CollectionGETBody, CollectionGETResponse, CollectionPOSTBody, CollectionPOSTResponse
)


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

    def authenticate(self, permission):
        """Authenticates request with a given permission setting

        Parameters
        ----------
        permission : str
            The required permission ["read", "write", "compute", "admin"]

        """
        if "Authorization" in self.request.headers:

            data = json.loads(self.request.headers["Authorization"])
            username = data["username"]
            password = data["password"]
        else:
            username = None
            password = None

        verified, msg = self.objects["storage_socket"].verify_user(username, password, permission)
        if verified is False:
            raise tornado.web.HTTPError(status_code=401, reason=msg)

    # def build_body(self, obj_type):
    #     try:
    #         return obj_type(self.rquest.body)
    #     except pydantic.ValidationError as e:
    #         raise tornado.web.HTTPError(status_code=401, reason=str(e))


class InformationHandler(APIHandler):
    """
    A handler that returns public server information
    """

    def get(self):
        """

        """
        self.authenticate("read")

        self.logger.info("GET: Information")

        self.write(self.objects["public_information"])


class MoleculeHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):
        """

        Experimental documentation, need to find a decent format.

        Request:
            "meta" - Overall options to the Molecule pull request
                - "index" - What kind of index used to find the data ("id", "molecule_hash", "molecular_formula")
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

        storage = self.objects["storage_socket"]

        body = MoleculeGETBody.parse_raw(self.request.body)

        molecules = storage.get_molecules(body.data, index=body.meta.index.value)
        self.logger.info("GET: Molecule - {} pulls.".format(len(molecules["data"])))

        ret = MoleculeGETResponse(**molecules)
        self.write(ret.json())

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

        storage = self.objects["storage_socket"]

        body = MoleculePOSTBody.parse_raw(self.request.body)
        ret = storage.add_molecules(body.data)
        response = MoleculePOSTResponse(**ret)

        self.logger.info("POST: Molecule - {} inserted.".format(response.meta.n_inserted))

        self.write(response.json())


class OptionHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):
        self.authenticate("read")

        storage = self.objects["storage_socket"]

        body = OptionGETBody.parse_raw(self.request.body)
        ret = storage.get_options(**body.data, with_ids=False)
        options = OptionGETResponse(**ret)

        self.logger.info("GET: Options - {} pulls.".format(len(options.data)))

        self.write(options.json())

    def post(self):
        self.authenticate("write")

        storage = self.objects["storage_socket"]

        body = OptionPOSTBody.parse_raw(self.request.body)
        ret = storage.add_options(body.data)
        response = OptionPOSTResponse(**ret)

        self.logger.info("POST: Options - {} inserted.".format(response.meta.n_inserted))

        self.write(response.json())


class CollectionHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):
        self.authenticate("read")

        storage = self.objects["storage_socket"]

        body = CollectionGETBody.parse_raw(self.request.body)
        cols = storage.get_collections(**body.data.dict())
        response = CollectionGETResponse(**cols)
        self.logger.info("GET: Options - {} pulls.".format(len(response.data)))

        self.write(response.json())

    def post(self):
        self.authenticate("write")

        storage = self.objects["storage_socket"]

        body = CollectionPOSTBody.parse_raw(self.request.body)
        ret = storage.add_collection(body.data.collection,
                                     body.data.name,
                                     body.data.dict(exclude={"collection", "name"}),
                                     overwrite=body.meta.overwrite)

        response = CollectionPOSTResponse(**ret)

        self.logger.info("POST: Collections - {} inserted.".format(response.meta.n_inserted))

        self.write(response.json())


class ResultHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):
        self.authenticate("read")

        storage = self.objects["storage_socket"]
        proj = self.json["meta"].get("projection", None)

        if "id" in self.json["data"]:
            ret = storage.get_results_by_id(self.json["data"]["id"], projection=proj)
        elif 'task_id' in self.json["data"]:
            ret = storage.get_results_by_task_id(self.json["data"]["task_id"], projection=proj)
        else:
            ret = storage.get_results(**self.json["data"], projection=proj)
        self.logger.info("GET: Results - {} pulls.".format(len(ret["data"])))

        self.write(ret)

    def post(self):
        self.authenticate("write")

        storage = self.objects["storage_socket"]

        ret = storage.add_results(self.json["data"])
        self.logger.info("POST: Results - {} inserted.".format(ret["meta"]["n_inserted"]))

        self.write(ret)


class ProcedureHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    def get(self):
        self.authenticate("read")

        storage = self.objects["storage_socket"]

        if "id" in self.json["data"]:
            ret = storage.get_procedures_by_id(id=self.json["data"]["id"])
        elif "hash_index" in self.json["data"]:
            ret = storage.get_procedures_by_id(hash_index=self.json["data"]["hash_index"])
        elif 'task_id' in self.json["data"]:
            ret = storage.get_procedures_by_task_id(self.json["data"]["task_id"])
        else:
            ret = storage.get_procedures(**self.json["data"])
        self.logger.info("GET: Procedures - {} pulls.".format(len(ret["data"])))

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
