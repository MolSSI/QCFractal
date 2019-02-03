"""
Utilities and base functions for Services.
"""

import abc
import json

from typing import Any, Dict, List
from pydantic import BaseModel


class BaseService(BaseModel, abc.ABC):

    storage_socket: Any

    # Base information requiered by the class
    id: str = None
    hash_index: str
    status: str
    service: str
    program: str
    procedure: str

    @classmethod
    @abc.abstractmethod
    def initialize_from_api(cls, storage_socket, meta, molecule):
        """
        Initalizes a Service from the API
        """

    def dict(self, include=None, exclude=None, by_alias=False) -> Dict[str, Any]:
        return BaseModel.dict(self, exclude={"storage_socket"})

    def json_dict(self) -> str:
        return json.loads(self.json())

    @abc.abstractmethod
    def iterate(self):
        """
        Takes a "step" of the service. Should return False if not finished
        """
