"""
Web handlers for the FractalServer.
"""
import json

import tornado.web

from pydantic import ValidationError
from qcelemental.util import serialize, deserialize

from .interface.models.rest_models import rest_model

_valid_encodings = {
    "application/json": "json",
    "application/json-ext": "json-ext",
    "application/msgpack-ext": "msgpack-ext",
}

class APIHandler(tornado.web.RequestHandler):
    """
    A requests handler for API calls.
    """

    # Admin authentication required by default
    _required_auth = "admin"
    _logging_param_counts = {}

    def initialize(self, **objects):
        """
        Initializes the request to JSON, adds objects, and logging.
        """


        self.content_type = "Not Provided"
        try:
            self.content_type = self.request.headers["Content-Type"]
            self.encoding = _valid_encodings[self.content_type]
        except KeyError:
            raise tornado.web.HTTPError(status_code=401, reason=f"Did not understand 'Content-Type': {self.content_type}")

        # Always reply in the format sent
        self.set_header("Content-Type", self.content_type)

        self.objects = objects
        self.storage = self.objects["storage_socket"]
        self.logger = objects["logger"]
        self.api_logger = objects["api_logger"]
        self.username = None

    def prepare(self):
        if self._required_auth:
            self.authenticate(self._required_auth)

        try:
            self.data = deserialize(self.request.body, self.encoding)
        except:
            raise tornado.web.HTTPError(status_code=401, reason="Could not deserialize body.")

    def on_finish(self):

        exclude_uris = ['/task_queue', '/service_queue', '/queue_manager']
        if self.api_logger and self.request.method == 'GET' \
                and self.request.uri not in exclude_uris:

            extra_params = self.data.copy()
            if self._logging_param_counts:
                for key in self._logging_param_counts:
                    if extra_params["data"].get(key, None):
                        extra_params["data"][key] = len(extra_params["data"][key])

            if "data" in extra_params:
                extra_params["data"] = {k: v for k, v in extra_params["data"].items() if v is not None}

            extra_params = json.dumps(extra_params)

            log = self.api_logger.get_api_access_log(request=self.request, extra_params=extra_params)
            self.storage.save_access(log)

        # self.logger.info('Done saving API access to the database')

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
            return model(**self.data)
        except ValidationError as exc:
            raise tornado.web.HTTPError(status_code=401, reason="Invalid REST")

    def write(self, data):
        if not isinstance(data, (str, bytes)):
            data = serialize(data, self.encoding)

        return super().write(data)


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
    _logging_param_counts = {"id"}

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

        ret = self.storage.get_kvstore(body.data.id)
        ret = response_model(**ret)

        self.logger.info("GET: KVStore - {} pulls.".format(len(ret.data)))
        self.write(ret)


class MoleculeHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"
    _logging_param_counts = {"id"}

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

        molecules = self.storage.get_molecules(**{**body.data.dict(), **body.meta.dict()})
        ret = response_model(**molecules)

        self.logger.info("GET: Molecule - {} pulls.".format(len(ret.data)))
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

        body_model, response_model = rest_model("molecule", "post")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.add_molecules(body.data)
        response = response_model(**ret)

        self.logger.info("POST: Molecule - {} inserted.".format(response.meta.n_inserted))
        self.write(response)


class KeywordHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"
    _logging_param_counts = {"id"}

    def get(self):

        body_model, response_model = rest_model("keyword", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_keywords(**{**body.data.dict(), **body.meta.dict()}, with_ids=False)
        response = response_model(**ret)

        self.logger.info("GET: Keywords - {} pulls.".format(len(response.data)))
        self.write(response)

    def post(self):
        self.authenticate("write")

        body_model, response_model = rest_model("keyword", "post")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.add_keywords(body.data)
        response = response_model(**ret)

        self.logger.info("POST: Keywords - {} inserted.".format(response.meta.n_inserted))
        self.write(response)


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
        self.write(response)

    def post(self):
        self.authenticate("write")

        body_model, response_model = rest_model("collection", "post")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
        response = response_model(**ret)

        self.logger.info("POST: Collections - {} inserted.".format(response.meta.n_inserted))
        self.write(response)


class ResultHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"
    _logging_param_counts = {"id", "molecule"}

    def get(self):

        body_model, response_model = rest_model("result", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_results(**{**body.data.dict(), **body.meta.dict()})
        result = response_model(**ret)

        self.logger.info("GET: Results - {} pulls.".format(len(result.data)))
        self.write(result)


class ProcedureHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"
    _logging_param_counts = {"id"}

    def get(self):

        body_model, response_model = rest_model("procedure", "get")
        body = self.parse_bodymodel(body_model)

        try:
            ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            raise tornado.web.HTTPError(status_code=401, reason=str(e))

        response = response_model(**ret)

        self.logger.info("GET: Procedures - {} pulls.".format(len(response.data)))
        self.write(response)
