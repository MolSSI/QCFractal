"""
Web handlers for the FractalServer.
"""
import json

import tornado.web
from pydantic import ValidationError
from qcelemental.util import deserialize, serialize

from .interface.models.rest_models import rest_model
from .storage_sockets.storage_utils import add_metadata_template

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
            # default to "application/json"
            self.content_type = self.request.headers.get("Content-Type", "application/json")
            self.encoding = _valid_encodings[self.content_type]
        except KeyError:
            raise tornado.web.HTTPError(
                status_code=401, reason=f"Did not understand 'Content-Type': {self.content_type}"
            )

        # Always reply in the format sent
        self.set_header("Content-Type", self.content_type)

        self.objects = objects
        self.storage = self.objects["storage_socket"]
        self.logger = objects["logger"]
        self.api_logger = objects["api_logger"]
        self.view_handler = objects["view_handler"]
        self.username = None

    def prepare(self):
        if self._required_auth:
            self.authenticate(self._required_auth)

        try:
            if (self.encoding == "json") and isinstance(self.request.body, bytes):
                blob = self.request.body.decode()
            else:
                blob = self.request.body

            if blob:
                self.data = deserialize(blob, self.encoding)
            else:
                self.data = None
        except:
            raise tornado.web.HTTPError(status_code=401, reason="Could not deserialize body.")

    def on_finish(self):

        exclude_uris = ["/task_queue", "/service_queue", "/queue_manager"]
        if self.api_logger and self.request.method == "GET" and self.request.uri not in exclude_uris:

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
        except ValidationError:
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
        """"""

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


class WavefunctionStoreHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"
    _logging_param_counts = {"id"}

    def get(self):

        body_model, response_model = rest_model("wavefunctionstore", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_wavefunction_store(body.data.id, include=body.meta.include)
        if len(ret["data"]):
            ret["data"] = ret["data"][0]
        ret = response_model(**ret)

        self.logger.info("GET: WavefunctionStore - 1 pull.")
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

    def get(self, collection_id=None, view_function=None):

        # List collections
        if (collection_id is None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")
            body = self.parse_bodymodel(body_model)

            cols = self.storage.get_collections(
                **body.data.dict(), include=body.meta.include, exclude=body.meta.exclude
            )
            response = response_model(**cols)

            self.logger.info("GET: Collections - {} pulls.".format(len(response.data)))
            self.write(response)
            return

        # Get specific collection
        elif (collection_id is not None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")

            body = self.parse_bodymodel(body_model)
            cols = self.storage.get_collections(
                **body.data.dict(), col_id=int(collection_id), include=body.meta.include, exclude=body.meta.exclude
            )
            response = response_model(**cols)

            self.logger.info("GET: Collections - {} pulls.".format(len(response.data)))
            self.write(response)
            return

        # View-backed function on collection
        elif (collection_id is not None) and (view_function is not None):
            body_model, response_model = rest_model(f"collection/{collection_id}/{view_function}", "get")
            body = self.parse_bodymodel(body_model)
            if self.view_handler is None:
                meta = {
                    "success": False,
                    "error_description": "Server does not support collection views.",
                    "errors": [],
                    "msgpacked_cols": [],
                }
                self.write(response_model(meta=meta, data=None))
                self.logger.info("GET: Collections - view request made, but server does not have a view_handler.")
                return

            result = self.view_handler.handle_request(collection_id, view_function, body.data.dict())
            response = response_model(**result)

            self.logger.info(f"GET: Collections - {collection_id} view {view_function} pulls.")
            self.write(response)
            return

        # Unreachable?
        else:
            body_model, response_model = rest_model("collection", "get")
            meta = add_metadata_template()
            meta["success"] = False
            meta["error_description"] = "GET request for view with no collection ID not understood."
            self.write(response_model(meta=meta, data=None))
            self.logger.info(
                "GET: Collections - collection id is None, but view function is not None (should be unreachable)."
            )
            return

    def post(self, collection_id=None, view_function=None):
        self.authenticate("write")

        body_model, response_model = rest_model("collection", "post")
        body = self.parse_bodymodel(body_model)

        # POST requests not supported for anything other than "/collection"
        if collection_id is not None or view_function is not None:
            meta = add_metadata_template()
            meta["success"] = False
            meta["error_description"] = "POST requests not supported for sub-resources of /collection"
            self.write(response_model(meta=meta, data=None))
            self.logger.info("POST: Collections - Access attempted on subresource.")
            return

        ret = self.storage.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
        response = response_model(**ret)

        self.logger.info("POST: Collections - {} inserted.".format(response.meta.n_inserted))
        self.write(response)

    def delete(self, collection_id, _):
        self.authenticate("write")

        body_model, response_model = rest_model(f"collection/{collection_id}", "delete")
        ret = self.storage.del_collection(col_id=collection_id)
        if ret == 0:
            self.logger.info(f"DELETE: Collections - Attempted to delete non-existent collection {collection_id}.")
            raise tornado.web.HTTPError(status_code=404, reason=f"Collection {collection_id} does not exist.")
        else:
            self.write(response_model(meta={"success": True, "errors": [], "error_description": False}))
            self.logger.info(f"DELETE: Collections - Deleted collection {collection_id}.")


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

    def get(self, query_type="get"):

        body_model, response_model = rest_model("procedure", query_type)
        body = self.parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("procedure", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            raise tornado.web.HTTPError(status_code=401, reason=str(e))

        response = response_model(**ret)

        self.logger.info("GET: Procedures - {} pulls.".format(len(response.data)))
        self.write(response)


class OptimizationHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"
    _logging_param_counts = {"id"}

    def get(self, query_type="get"):

        body_model, response_model = rest_model(f"optimization/{query_type}", "get")
        body = self.parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("optimization", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            raise tornado.web.HTTPError(status_code=401, reason=str(e))

        response = response_model(**ret)

        self.logger.info("GET: Optimization ({}) - {} pulls.".format(query_type, len(response.data)))
        self.write(response)
