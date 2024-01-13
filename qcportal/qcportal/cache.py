"""
Caching for the PortalClient
"""

from __future__ import annotations

import datetime
import functools
import os
import sqlite3
from typing import TYPE_CHECKING, Optional, TypeVar, Type, Any, List, Iterable, Tuple
from urllib.parse import urlparse

from .utils import chunk_iterable

try:
    import pydantic.v1 as pydantic
    from pydantic.v1 import BaseModel, Extra, validator, PrivateAttr, Field
except ImportError:
    import pydantic
    from pydantic import BaseModel, Extra, validator, PrivateAttr, Field
import zstandard

from .serialization import serialize, deserialize

if TYPE_CHECKING:
    from qcportal.record_models import RecordStatusEnum

_DATASET_T = TypeVar("_DATASET_T")

_query_chunk_size = 125


def compress_for_cache(data: Any) -> sqlite3.Binary:
    serialized_data = serialize(data, "msgpack")
    compressed_data = zstandard.compress(serialized_data, level=1)
    return sqlite3.Binary(compressed_data)


def decompress_from_cache(data: sqlite3.Binary, value_type) -> Any:
    decompressed_data = zstandard.decompress(bytes(data))
    deserialized_data = deserialize(decompressed_data, "msgpack")
    return pydantic.parse_obj_as(value_type, deserialized_data)


class DatasetCache:
    def __init__(self, file_path: Optional[str], dataset_type: Type[_DATASET_T], read_only: bool):
        if file_path is None and read_only:
            raise RuntimeError("Cannot open a read-only memory-backed cache")
        if read_only and not os.path.isfile(file_path):
            raise RuntimeError("Cannot open existing read-only cache - file does not exist or is not a file")

        if file_path is None:
            file_path = ":memory:"

        self.file_path = file_path
        self.read_only = read_only
        self._entry_type = dataset_type._entry_type
        self._specification_type = dataset_type._specification_type
        self._record_type = dataset_type._record_type

        self._conn = sqlite3.connect(self.file_path)

        # Some common settings
        self._conn.execute("PRAGMA foreign_keys = ON")

        if not read_only:
            self._create_tables()

    def _assert_writable(self):
        assert not self.read_only, "This dataset cache is read-only"

    def _create_tables(self):
        self._assert_writable()

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_metadata (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )
        """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS entries (
                name TEXT PRIMARY KEY,
                entry BLOB NOT NULL
            )
        """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS specifications (
                name TEXT PRIMARY KEY,
                specification BLOB NOT NULL
            )
        """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS record_data (
                uid INTEGER PRIMARY KEY AUTOINCREMENT,
                id INTEGER NOT NULL,
                entry_name TEXT NOT NULL,
                specification_name TEXT NOT NULL,
                status TEXT NOT NULL,
                modified_on INTEGER NOT NULL,
                record BLOB NOT NULL,
                UNIQUE(id),
                FOREIGN KEY (entry_name) REFERENCES entries(name) ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (specification_name) REFERENCES specifications(name) ON DELETE CASCADE ON UPDATE CASCADE
            )
        """
        )

        self._conn.execute("CREATE INDEX IF NOT EXISTS record_data_entry_name ON record_data (entry_name)")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS record_data_specification_name ON record_data (specification_name)"
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS record_data_status ON record_data (status)")
        self._conn.commit()

    def update_metadata(self, key: str, value: Any) -> None:
        stmt = "REPLACE INTO dataset_metadata (key, value) VALUES (?, ?)"
        self._conn.execute(stmt, (key, serialize(value, "msgpack")))
        self._conn.commit()

    def get_metadata(self, key) -> Any:
        stmt = "SELECT value FROM dataset_metadata WHERE key = ?"
        r = self._conn.execute(stmt, (key,)).fetchone()
        return deserialize(r[0], "msgpack")

    def entry_exists(self, name: str) -> bool:
        stmt = "SELECT 1 FROM entries WHERE name=?"
        return self._conn.execute(stmt, (name,)).fetchone() is not None

    def get_entry_names(self) -> List[str]:
        stmt = "SELECT name FROM entries"
        return [x[0] for x in self._conn.execute(stmt).fetchall()]

    def get_entry(self, name: str) -> Optional[BaseModel]:
        stmt = "SELECT entry FROM entries WHERE name=?"
        entry_data = self._conn.execute(stmt, (name,)).fetchone()
        if entry_data is None:
            return None
        return decompress_from_cache(entry_data[0], self._entry_type)

    def get_entries(self, names: Iterable[str]) -> List[BaseModel]:
        all_entries = []
        for names_batch in chunk_iterable(names, _query_chunk_size):
            name_param = ",".join("?" * len(names_batch))
            stmt = f"""SELECT entry FROM entries WHERE name IN ({name_param})"""
            entry_data = self._conn.execute(stmt, (*names_batch,)).fetchall()
            all_entries.extend(decompress_from_cache(x[0], self._entry_type) for x in entry_data)
        return all_entries

    def update_entries(self, entries: Iterable[BaseModel]):
        self._assert_writable()

        assert all(isinstance(e, self._entry_type) for e in entries)

        for entry in entries:
            stmt = "REPLACE INTO entries (name, entry) VALUES (?, ?)"
            row_data = (entry.name, compress_for_cache(entry))
            self._conn.execute(stmt, row_data)

        self._conn.commit()

    def rename_entry(self, old_name: str, new_name: str):
        self._assert_writable()

        entry = self.get_entry(old_name)
        if entry is None:  # does not exist
            return

        entry.name = new_name

        stmt = "UPDATE entries SET name=?, entry=? WHERE name=?"
        self._conn.execute(stmt, (new_name, compress_for_cache(entry), old_name))
        self._conn.commit()

    def delete_entry(self, name):
        self._assert_writable()

        stmt = "DELETE FROM entries WHERE name=?"
        self._conn.execute(stmt, (name,))
        self._conn.commit()

    def specification_exists(self, name: str) -> bool:
        stmt = "SELECT 1 FROM specifications WHERE name=?"
        return self._conn.execute(stmt, (name,)).fetchone() is not None

    def get_specification_names(self) -> List[str]:
        stmt = "SELECT name FROM specifications"
        return [x[0] for x in self._conn.execute(stmt).fetchall()]

    def get_specification(self, name: str):
        stmt = "SELECT specification FROM specifications WHERE name=?"
        spec_data = self._conn.execute(stmt, (name,)).fetchone()
        if spec_data is None:
            return None
        return decompress_from_cache(spec_data[0], self._specification_type)

    def get_all_specifications(self) -> List[BaseModel]:
        stmt = "SELECT specification FROM specifications"
        spec_data = self._conn.execute(stmt).fetchall()
        return [decompress_from_cache(x[0], self._specification_type) for x in spec_data]

    def get_specifications(self, names: Iterable[str]) -> List[BaseModel]:
        name_param = ",".join("?" * len(names))

        stmt = f"""SELECT specification FROM specifications WHERE name IN ({name_param})"""
        entry_data = self._conn.execute(stmt, (*names,)).fetchall()
        return [decompress_from_cache(x[0], self._specification_type) for x in entry_data]

    def update_specifications(self, specifications: Iterable[BaseModel]):
        self._assert_writable()

        assert all(isinstance(s, self._specification_type) for s in specifications)

        for specification in specifications:
            stmt = "REPLACE INTO specifications (name, specification) VALUES (?, ?)"
            row_data = (specification.name, compress_for_cache(specification))
            self._conn.execute(stmt, row_data)

        self._conn.commit()

    def rename_specification(self, old_name: str, new_name: str):
        self._assert_writable()

        specification = self.get_specification(old_name)
        if specification is None:  # does not exist
            return

        specification.name = new_name

        stmt = "UPDATE specifications SET name=?, specification=? WHERE name=?"
        self._conn.execute(stmt, (new_name, compress_for_cache(specification), old_name))
        self._conn.commit()

    def delete_specification(self, name):
        self._assert_writable()

        stmt = "DELETE FROM specifications WHERE name=?"
        self._conn.execute(stmt, (name,))
        self._conn.commit()

    def record_exists(self, entry_name: str, specification_name: str) -> bool:
        stmt = "SELECT 1 FROM record_data WHERE entry_name=? and specification_name=?"
        return self._conn.execute(stmt, (entry_name, specification_name)).fetchone() is not None

    def get_record(self, entry_name: str, specification_name: str):
        stmt = "SELECT uid, record FROM record_data WHERE entry_name=? and specification_name=?"
        record_data = self._conn.execute(stmt, (entry_name, specification_name)).fetchone()
        if record_data is None:
            return None

        record = decompress_from_cache(record_data[1], self._record_type)

        if not self.read_only:
            record._del_tasks.append(functools.partial(self.writeback_record, record_data[0]))  # give it the uid

        return record

    def get_records(self, entry_names: Iterable[str], specification_names: Iterable[str]):
        specification_params = ",".join("?" * len(specification_names))
        all_records = []

        for entry_names_batch in chunk_iterable(entry_names, _query_chunk_size):
            entry_params = ",".join("?" * len(entry_names_batch))

            stmt = f"""SELECT uid, entry_name, specification_name, record
                       FROM record_data
                       WHERE entry_name IN ({entry_params})
                       AND specification_name IN ({specification_params})"""

            all_params = (*entry_names_batch, *specification_names)
            rdata = self._conn.execute(stmt, all_params).fetchall()

            for uid, ename, sname, compressed_record in rdata:
                record = decompress_from_cache(compressed_record, self._record_type)

                if not self.read_only:
                    record._del_tasks.append(functools.partial(self.writeback_record, uid))  # give it the uid

                all_records.append((ename, sname, record))

        return all_records

    def update_records(self, record_info: Iterable[Tuple[str, str, Any]]):
        self._assert_writable()

        assert all(isinstance(r, self._record_type) for _, _, r in record_info)

        for entry_name, specification_name, record in record_info:
            stmt = """REPLACE INTO record_data (id, entry_name, specification_name, status, modified_on, record)
                      VALUES (?, ?, ?, ?, ?, ?)"""

            row_data = (
                record.id,
                entry_name,
                specification_name,
                record.status,
                record.modified_on.timestamp(),
                compress_for_cache(record),
            )
            self._conn.execute(stmt, row_data)

        self._conn.commit()

    def writeback_record(self, uid, record):
        self._assert_writable()

        assert isinstance(record, self._record_type)

        compressed_record = compress_for_cache(record)

        # Only update if ids and uid match, and if this record is larger
        # than what is stored already
        # The record (based on uid) may not exist anymore in the cache, but that is ok
        stmt = """UPDATE record_data SET record = ?
                  WHERE uid = ? AND id = ? AND length(record) < ?"""

        row_data = (compressed_record, uid, record.id, len(compressed_record))
        self._conn.execute(stmt, row_data)
        self._conn.commit()

    def delete_record(self, entry_name: str, specification_name: str):
        self._assert_writable()

        stmt = "DELETE FROM record_data WHERE entry_name=? AND specification_name=?"
        self._conn.execute(stmt, (entry_name, specification_name))
        self._conn.commit()

    def delete_records(self, entry_names: Optional[Iterable[str]], specification_names: Optional[Iterable[str]]):
        self._assert_writable()

        all_params = []
        conds = []

        stmt = f"DELETE FROM record_data "

        if entry_names is not None:
            entry_params = ",".join("?" * len(entry_names))
            all_params.extend(entry_names)
            conds.append(f" entry_name IN ({entry_params})")

        if specification_names is not None:
            specification_params = ",".join("?" * len(specification_names))
            all_params.extend(specification_names)
            conds.append(f" specification_name IN ({specification_params})")

        if conds:
            stmt += " WHERE " + " AND ".join(conds)

        self._conn.execute(stmt, all_params)
        self._conn.commit()

    def get_record_info(
        self,
        entry_names: Iterable[str],
        specification_names: Iterable[str],
        status: Optional[Iterable[RecordStatusEnum]],
    ) -> List[Tuple[str, str, int, RecordStatusEnum, datetime.datetime]]:
        all_info = []
        specification_params = ",".join("?" * len(specification_names))

        if status is not None:
            status_params = ",".join("?" * len(status))
        else:
            status_params = None

        for entry_names_batch in chunk_iterable(entry_names, _query_chunk_size):
            entry_params = ",".join("?" * len(entry_names_batch))

            stmt = f"""SELECT entry_name, specification_name, id, status, modified_on
                       FROM record_data
                       WHERE entry_name IN ({entry_params})
                       AND specification_name IN ({specification_params})"""

            if status_params is not None:
                stmt = stmt + f"AND status IN ({status_params})"
                all_params = (*entry_names_batch, *specification_names, *status)
            else:
                all_params = (*entry_names_batch, *specification_names)

            rinfo = self._conn.execute(stmt, all_params).fetchall()
            all_info.extend(
                (e, s, id, status, datetime.datetime.fromtimestamp(modified_on, tz=datetime.timezone.utc))
                for e, s, id, status, modified_on in rinfo
            )

        return all_info

    def get_existing_records(
        self, entry_names: Iterable[str], specification_names: Iterable[str]
    ) -> List[Tuple[str, str, int]]:
        # Seem hacky, but searching around this seems to be the right way to do this
        entry_params = ",".join("?" * len(entry_names))
        specification_params = ",".join("?" * len(specification_names))

        stmt = f"""SELECT entry_name, specification_name, id
                   FROM record_data
                   WHERE entry_name IN ({entry_params})
                   AND specification_name IN ({specification_params})"""

        return self._conn.execute(stmt, (*entry_names, *specification_names)).fetchall()


class PortalCache:
    def __init__(self, server_uri: str, cache_dir: Optional[str], max_size: int):
        parsed_url = urlparse(server_uri)

        # Should work as a reasonable fingerprint?
        self.server_fingerprint = f"{parsed_url.hostname}_{parsed_url.port}"

        if cache_dir:
            self.enabled = True

            self.cache_dir = os.path.join(os.path.abspath(cache_dir), self.server_fingerprint)
            os.makedirs(self.cache_dir, exist_ok=True)

        else:
            self.enabled = False
            self.cache_dir = None

    def get_cache_path(self, cache_name: str) -> str:
        if self.enabled:
            return os.path.join(self.cache_dir, f"{cache_name}.sqlite")
        else:
            return ":memory:"

    def vacuum(self, cache_name: Optional[str] = None):
        if self.enabled:
            # TODO
            return


def read_dataset_metadata(file_path: str):
    """
    Reads the type of dataset stored in a cache file

    This is needed sometimes to construct the DatasetCache object with a type
    """

    if not os.path.isfile(file_path):
        raise RuntimeError(f'Cannot open cache file "{file_path}" - does not exist or is not a file')
    conn = sqlite3.connect(file_path)
    r = conn.execute("SELECT value FROM dataset_metadata WHERE key = 'dataset_metadata'")
    if r is None:
        raise RuntimeError(f"Cannot find appropriate metadata in cache file {file_path}")

    d = deserialize(r.fetchone()[0], "msgpack")
    conn.close()
    return d
