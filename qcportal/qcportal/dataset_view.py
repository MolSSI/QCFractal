from __future__ import annotations

import os
import sqlite3
from typing import Optional, Dict, Any, List, Iterable

import zstandard

try:
    from pydantic.v1 import BaseModel, validator, PrivateAttr, parse_obj_as, Extra
except ImportError:
    from pydantic import BaseModel, validator, PrivateAttr, parse_obj_as, Extra

from qcportal.serialization import deserialize


class DatasetViewWrapper(BaseModel):
    class Config:
        extra = Extra.forbid

    view_path: str
    _sqlite_con = PrivateAttr()

    def __init__(self, **data):
        BaseModel.__init__(self, **data)
        self._sqlite_con = sqlite3.connect(self.view_path)

    @validator("view_path")
    def validate_path(cls, v):
        if not os.path.isfile(v):
            raise RuntimeError(f"View file {v} does not exist or is not a file")
        return os.path.abspath(v)

    @staticmethod
    def deserialize_dict(data_bytes) -> Dict[str, Any]:
        data_decompressed = zstandard.decompress(data_bytes)
        return deserialize(data_decompressed, "application/msgpack")

    @staticmethod
    def deserialize_model(data_bytes, model):
        data_dict = DatasetViewWrapper.deserialize_dict(data_bytes)
        return parse_obj_as(model, data_dict)

    def get_datamodel(self) -> Dict[str, Any]:
        # Read raw_data (datamodel)
        cur = self._sqlite_con.cursor()
        raw_data_bytes = cur.execute("SELECT value FROM dataset_metadata WHERE key = 'raw_data'").fetchone()[0]
        return self.deserialize_dict(raw_data_bytes)

    def get_entry_names(self) -> List[str]:
        cur = self._sqlite_con.cursor()
        entry_names = cur.execute("SELECT name FROM dataset_entry")
        return [x[0] for x in entry_names]

    def get_specifications(self, specification_type) -> Dict[str, Any]:
        cur = self._sqlite_con.cursor()

        ret = {}
        spec_info = cur.execute("SELECT name, data FROM dataset_specification")

        for specification_name, specification_bytes in spec_info:
            ret[specification_name] = self.deserialize_model(specification_bytes, specification_type)

        return ret

    def get_entries(self, entry_type, entry_names: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        cur = self._sqlite_con.cursor()

        ret = {}
        if entry_names is None:
            entry_info = cur.execute("SELECT name, data FROM dataset_entry")

            for entry_name, entry_bytes in entry_info:
                ret[entry_name] = self.deserialize_model(entry_bytes, entry_type)
        else:
            # Doing a query with IN is kind of a pain. Just go one by one
            entry_names = set(entry_names)
            stmt = "SELECT name, data FROM dataset_entry WHERE name = (:entry_name)"

            for entry_name in entry_names:
                entry_name, entry_bytes = cur.execute(stmt, {"entry_name": entry_name}).fetchone()
                ret[entry_name] = self.deserialize_model(entry_bytes, entry_type)

        return ret

    def get_record_item(self, record_item_type, entry_name: str, specification_name: str):
        cur = self._sqlite_con.cursor()
        stmt = """SELECT data FROM dataset_record
                  WHERE entry_name = (:entry_name) AND specification_name = (:specification_name)"""
        params = {"entry_name": entry_name, "specification_name": specification_name}
        record_item = cur.execute(stmt, params).fetchone()
        if record_item is None:
            return None
        else:
            return self.deserialize_model(record_item[0], record_item_type)

    def iterate_records(self, record_item_type):
        cur = self._sqlite_con.cursor()
        r = cur.execute("SELECT entry_name, specification_name, data FROM dataset_record")

        for row in r:
            if row is None:
                break

            entry_name, spec_name, record_item_data = row
            record_item = self.deserialize_model(record_item_data, record_item_type)
            record = record_item.record
            yield entry_name, spec_name, record
