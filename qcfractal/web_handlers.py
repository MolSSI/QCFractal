"""
Web handlers for the FractalServer.
"""
import json

import tornado.web

from pydantic import ValidationError

from .interface.models.rest_models import rest_model


class APIHandler(tornado.web.RequestHandler):
    """
    A requests handler for API calls.
    """

    # Admin authentication required by default
    _required_auth = "admin"

    def initialize(self, **objects):
        """
        Initializes the request to JSON, adds objects, and logging.
        """

        self.set_header("Content-Type", "application/json")
        self.objects = objects
        self.storage = self.objects["storage_socket"]
        self.logger = objects["logger"]
        self.username = None

    def prepare(self):
        if self._required_auth:
            self.authenticate(self._required_auth)

        self.json = json.loads(self.request.body.decode("UTF-8"))

    def authenticate(self, permission):
        """Authenticates request with a given permission setting.

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

        self.username = username

        verified, msg = self.objects["storage_socket"].verify_user(username, password, permission)
        if verified is False:
            raise tornado.web.HTTPError(status_code=401, reason=msg)

    def parse_bodymodel(self, model):

        try:
            return model.parse_raw(self.request.body)
        except ValidationError as exc:
            raise tornado.web.HTTPError(status_code=401, reason="Invalid REST")


class InformationHandler(APIHandler):
    """
    A handler that returns public server information.
    """

    _required_auth = "read"

    def get(self):
        """

        """

        self.logger.info("GET: Information")

        self.write(self.objects["public_information"])


class KVStoreHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):
        """

        Experimental documentation, need to find a decent format.

        Request:
            "data" - A list of key requests

        Returns:
            "meta" - Metadata associated with the query
                - "errors" - A list of errors in (index, error_id) format.
                - "n_found" - The number of molecule found.
                - "success" - If the query was successful or not.
                - "error_description" - A string based description of the error or False
                - "missing" - A list of keys that were not found.
            "data" - A dictionary of {key : value} dictionary of the results

        """

        body_model, response_model = rest_model("kvstore", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_kvstore(body.data)
        ret = response_model(**ret)

        self.logger.info("GET: KVStore - {} pulls.".format(len(ret.data)))
        self.write(ret.json())


class MoleculeHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

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

        body_model, response_model = rest_model("molecule", "get")
        body = self.parse_bodymodel(body_model)

        molecules = self.storage.get_molecules(**body.data.dict())
        ret = response_model(**molecules)

        self.logger.info("GET: Molecule - {} pulls.".format(len(ret.data)))
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

        body_model, response_model = rest_model("molecule", "post")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.add_molecules(body.data)
        response = response_model(**ret)

        self.logger.info("POST: Molecule - {} inserted.".format(response.meta.n_inserted))
        self.write(response.json())


class KeywordHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):

        body_model, response_model = rest_model("keyword", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_keywords(**body.data.dict(), with_ids=False)
        response = response_model(**ret)

        self.logger.info("GET: Keywords - {} pulls.".format(len(response.data)))
        self.write(response.json())

    def post(self):
        self.authenticate("write")

        body_model, response_model = rest_model("keyword", "post")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.add_keywords(body.data)
        response = response_model(**ret)

        self.logger.info("POST: Keywords - {} inserted.".format(response.meta.n_inserted))
        self.write(response.json())


class CollectionHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):


        body_model, response_model = rest_model("collection", "get")
        body = self.parse_bodymodel(body_model)

        cols = self.storage.get_collections(**body.data.dict(), projection=body.meta.projection)
        response = response_model(**cols)

        self.logger.info("GET: Collections - {} pulls.".format(len(response.data)))
        self.write(response.json())

    def post(self):
        self.authenticate("write")

        body_model, response_model = rest_model("collection", "post")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
        response = response_model(**ret)

        self.logger.info("POST: Collections - {} inserted.".format(response.meta.n_inserted))
        self.write(response.json())


class ResultHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):

        body_model, response_model = rest_model("result", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_results(**body.data.dict(), projection=body.meta.projection)
        result = response_model(**ret)

        self.logger.info("GET: Results - {} pulls.".format(len(result.data)))
        self.write(result.json())


class ProcedureHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):

        body_model, response_model = rest_model("procedure", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_procedures(**body.data.dict())
        response = response_model(**ret)

        self.logger.info("GET: Procedures - {} pulls.".format(len(response.data)))
        self.write(response.json())
