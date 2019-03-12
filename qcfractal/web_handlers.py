"""
Web handlers for the FractalServer
"""
import json

import tornado.web

from .interface.models.rest_models import (MoleculeGETBody, MoleculeGETResponse, MoleculePOSTBody,
                                           MoleculePOSTResponse, KeywordGETBody, KeywordGETResponse, KeywordPOSTBody,
                                           KeywordPOSTResponse, KVStoreGETBody, KVStoreGETResponse, CollectionGETBody,
                                           CollectionGETResponse, CollectionPOSTBody, CollectionPOSTResponse,
                                           ResultGETBody, ResultGETResponse, ProcedureGETBody, ProcedureGETReponse)


class APIHandler(tornado.web.RequestHandler):
    """
    A requests handler for API calls, build
    """

    # Admin authentication required by default
    _required_auth = "admin"

    def initialize(self, **objects):
        """
        Initializes the request to JSON, adds objects, and logging.
        """

        self.set_header("Content-Type", "application/json")
        self.objects = objects
        self.logger = objects["logger"]

    def prepare(self):
        if self._required_auth:
            self.authenticate(self._required_auth)

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


class InformationHandler(APIHandler):
    """
    A handler that returns public server information
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

        storage = self.objects["storage_socket"]

        body = KVStoreGETBody.parse_raw(self.request.body)

        ret = storage.get_kvstore(body.data)
        self.logger.info("GET: KVStore - {} pulls.".format(len(ret["data"])))

        ret = KVStoreGETResponse(**ret)
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

        storage = self.objects["storage_socket"]

        body = MoleculeGETBody.parse_raw(self.request.body)

        molecules = storage.get_molecules(**body.data)
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

    _required_auth = "read"

    def get(self):

        storage = self.objects["storage_socket"]

        body = KeywordGETBody.parse_raw(self.request.body)
        ret = storage.get_keywords(**body.data, with_ids=False)
        kw = KeywordGETResponse(**ret)

        self.logger.info("GET: Keywords - {} pulls.".format(len(kw.data)))

        self.write(kw.json())

    def post(self):
        self.authenticate("write")

        storage = self.objects["storage_socket"]

        body = KeywordPOSTBody.parse_raw(self.request.body)
        ret = storage.add_keywords(body.data)
        response = KeywordPOSTResponse(**ret)

        self.logger.info("POST: Keywords - {} inserted.".format(response.meta.n_inserted))

        self.write(response.json())


class CollectionHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):

        storage = self.objects["storage_socket"]

        body = CollectionGETBody.parse_raw(self.request.body)
        cols = storage.get_collections(**body.data.dict(), projection=body.meta.projection)
        response = CollectionGETResponse(**cols)
        self.logger.info("GET: Collections - {} pulls.".format(len(response.data)))

        self.write(response.json())

    def post(self):
        self.authenticate("write")

        storage = self.objects["storage_socket"]

        body = CollectionPOSTBody.parse_raw(self.request.body)
        ret = storage.add_collection(
            body.data.dict(),
            overwrite=body.meta.overwrite)

        response = CollectionPOSTResponse(**ret)

        self.logger.info("POST: Collections - {} inserted.".format(response.meta.n_inserted))

        self.write(response.json())


class ResultHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):

        storage = self.objects["storage_socket"]

        body = ResultGETBody.parse_raw(self.request.body)
        proj = body.meta.projection
        if 'id' in body.data:
            ret = storage.get_results(id=body.data['id'], projection=proj)
        elif 'task_id' in body.data:
            ret = storage.get_results(task_id=body.data['task_id'], projection=proj)
        else:
            ret = storage.get_results(**body.data, projection=proj)
        result = ResultGETResponse(**ret)
        self.logger.info("GET: Results - {} pulls.".format(len(result.data)))
        self.write(result.json())


class ProcedureHandler(APIHandler):
    """
    A handler to push and get molecules.
    """

    _required_auth = "read"

    def get(self):

        storage = self.objects["storage_socket"]

        body = ProcedureGETBody.parse_raw(self.request.body)

        ret = storage.get_procedures(**body.data)

        response = ProcedureGETReponse(**ret)
        self.logger.info("GET: Procedures - {} pulls.".format(len(response.data)))

        self.write(response.json())
