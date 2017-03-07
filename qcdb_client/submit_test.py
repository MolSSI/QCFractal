
import os
import sys
from uuid import uuid4

try:
    from urllib.parse import quote
except ImportError:
    # Python 2.
    from urllib import quote

from tornado import gen, httpclient, ioloop
from tornado.options import define, options
import json


@gen.coroutine
def post(json_data):
    client = httpclient.AsyncHTTPClient()
    data = json.dumps(json_data)
    response = yield client.fetch('http://localhost:8888/post',
                                  method='POST',
                                  body=data)

    print(response)



data = {"key": "something", "values":[3, 2], "other":5}
ioloop.IOLoop.current().run_sync(lambda: post(data))
