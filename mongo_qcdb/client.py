"""Provides an interface the QCDB Server instance"""

import json
from tornado import gen, httpclient, ioloop
from . import mongo_helper


class Client(object):
    def __init__(self, port):
        if "http" not in port:
            port = "http://" + port
        self.port = port + '/'

        self.info = self.get_information()

    def get_MongoSocket(self):
        """
        Builds a new MongoSocket from the internal data.
        """
        return mongo_helper.MongoSocket(*self.info["mongo_data"])

    @gen.coroutine
    def _query_server(self, function, method, body=None):
        """
        Basic non-blocking server query.
        """
        if body is not None:
            body = json.dumps(body)

        client = httpclient.AsyncHTTPClient()
        yield json.loads(client.fetch(self.port + function, method=method).body.decode('utf-8'))

    def query_server(self, function, method, body=None):
        """
        Basic blocking server query.
        """
        if body is not None:
            body = json.dumps(body)

        client = httpclient.HTTPClient()
        response = client.fetch(self.port + function, method=method, body=body)
        return json.loads(response.body.decode('utf-8'))

    def get_information(self):
        return self.query_server("information", "GET")

    def submit_task(self, json_data):
        return self.query_server("scheduler", "POST", body=json_data)

    def get_queue(self):
        return self.query_server("scheduler", "GET")
