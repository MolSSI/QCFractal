"""Provides an interface the QCDB Server instance"""

import json
import requests
import pandas as pd

class Portal(object):
    def __init__(self, port, username="", password=""):
        if "http" not in port:
            port = "http://" + port
        self.port = port + '/'
        self.project = project
        self.http_header = {"project": self.project, "username": username, "password": password}
        # self.info = self.get_information()

    # def get_molcules(self, )
