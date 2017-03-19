"""Provides an interface the QCDB Server instance"""

import json
from tornado import gen, httpclient, ioloop
from . import mongo_helper


class Client(object):
    def __init__(self, port, project="default"):
        if "http" not in port:
            port = "http://" + port
        self.port = port + '/'
        self.project = project
        self.info = self.get_information()

    def get_MongoSocket(self):
        """
        Builds a new MongoSocket from the internal data.
        """
        print(self.info["mongo_data"])
        socket = mongo_helper.MongoSocket(*self.info["mongo_data"])
        socket.set_project(self.project)
        return socket

    @gen.coroutine
    def _query_server(self, function, method, body=None, project=None):
        """
        Basic non-blocking server query.
        """
        if body is not None:
            body = json.dumps(body)

        if project is None:
            project = self.project

        client = httpclient.AsyncHTTPClient()
        http_header = {"project" : project}
        yield json.loads(client.fetch(self.port + function, method=method, headers=http_header).body.decode('utf-8'))

    def query_server(self, function, method, body=None, project=None):
        """
        Basic blocking server query.
        """
        if body is not None:
            body = json.dumps(body)

        if project is None:
            project = self.project

        client = httpclient.HTTPClient()
        http_header = {"project" : project}
        response = client.fetch(self.port + function, method=method, body=body, headers=http_header)
        return json.loads(response.body.decode('utf-8'))

    def get_information(self):
        return self.query_server("information", "GET")

    def submit_task(self, json_data):
        return self.query_server("scheduler", "POST", body=json_data)

    def get_queue(self):
        return self.query_server("scheduler", "GET")
