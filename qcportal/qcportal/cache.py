"""
Caching for the PortalClient
"""

from __future__ import annotations

import datetime
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
_RECORD_T = TypeVar("_RECORD_T")

_query_chunk_size = 125


def compress_for_cache(data: Any) -> sqlite3.Binary:
    serialized_data = serialize(data, "msgpack")
    compressed_data = zstandard.compress(serialized_data, level=1)
    return sqlite3.Binary(compressed_data)


def decompress_from_cache(data: sqlite3.Binary, value_type) -> Any:
    decompressed_data = zstandard.decompress(bytes(data))
    deserialized_data = deserialize(decompressed_data, "msgpack")
    return pydantic.parse_obj_as(value_type, deserialized_data)


class RecordCache:
    def __init__(self, file_path: Optional[str], read_only: bool):
        if file_path is None and read_only:
            raise RuntimeError("Cannot open a read-only memory-backed cache")
        if read_only and not os.path.isfile(file_path):
            raise RuntimeError("Cannot open existing read-only cache - file does not exist or is not a file")

        if file_path is None:
            file_path = ":memory:"

        self.file_path = file_path
        self.read_only = read_only

        self._conn = sqlite3.connect(self.file_path)

        # Some common settings
        self._conn.execute("PRAGMA foreign_keys = ON")

        if not read_only:
            self._create_tables()
            self._conn.commit()

    def __str__(self):
        return f"<{self.__class__.__name__} path={self.file_path} {'ro' if self.read_only else 'rw'}>"

    def _assert_writable(self):
        assert not self.read_only, "This dataset cache is read-only"

    def _create_tables(self):
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                uid INTEGER PRIMARY KEY AUTOINCREMENT,
                id INTEGER NOT NULL,
                status TEXT NOT NULL,
                modified_on INTEGER NOT NULL,
                record BLOB NOT NULL,
                UNIQUE(id)
            )
            """
        )

        self._conn.execute("CREATE INDEX IF NOT EXISTS records_status ON records (status)")

    def update_metadata(self, key: str, value: Any) -> None:
        stmt = "REPLACE INTO metadata (key, value) VALUES (?, ?)"
        self._conn.execute(stmt, (key, serialize(value, "msgpack")))
        self._conn.commit()

    def get_record(self, record_id: int, record_type: Type[_RECORD_T]) -> Optional[_RECORD_T]:
        stmt = "SELECT uid, record FROM records WHERE id = ?"

        record_data = self._conn.execute(stmt, (record_id,)).fetchone()
        if record_data is None:
            return None

        record = decompress_from_cache(record_data[1], record_type)

        record._record_cache = self
        record._record_cache_uid = record_data[0]

        return record

    def get_records(self, record_ids: Iterable[int], record_type: Type[_RECORD_T]) -> List[_RECORD_T]:
        all_records = []

        for record_id_batch in chunk_iterable(record_ids, _query_chunk_size):
            id_params = ",".join("?" * len(record_id_batch))
            stmt = f"SELECT uid, record FROM records WHERE id IN ({id_params})"

            rdata = self._conn.execute(stmt, record_id_batch).fetchall()

            for uid, compressed_record in rdata:
                record = decompress_from_cache(compressed_record, record_type)

                record._record_cache = self
                record._record_cache_uid = uid

                all_records.append(record)

        return all_records

    def get_existing_records(self, record_ids: Iterable[int]) -> List[int]:
        ret = []
        for record_id_batch in chunk_iterable(record_ids, _query_chunk_size):
            record_id_params = ",".join("?" * len(record_id_batch))

            stmt = f"SELECT id FROM records WHERE id IN ({record_id_params})"

            r = self._conn.execute(stmt, record_id_batch).fetchall()
            ret.extend(r)

        return ret

    def update_records(self, records: Iterable[_RECORD_T]):
        # TODO - update multiple at once (VALUES ((?, ?, ?, ?), (?, ?, ?, ?))
        stmt = "REPLACE INTO records (id, status, modified_on, record) VALUES (?, ?, ?, ?) RETURNING uid"

        uids = []
        for record in records:
            r = self._conn.execute(
                stmt, (record.id, record.status, record.modified_on.timestamp(), compress_for_cache(record))
            )
            uids.append(r.fetchone()[0])

        self._conn.commit()
        assert None not in uids
        return uids

    def writeback_record(self, uid, record):
        self._assert_writable()

        compressed_record = compress_for_cache(record)

        # Only update if ids and uid match, and if this record is larger
        # than what is stored already
        # The record (based on uid) may not exist anymore in the cache, but that is ok
        stmt = """UPDATE records SET record = ?
                  WHERE uid = ? AND id = ? AND length(record) < ?"""

        row_data = (compressed_record, uid, record.id, len(compressed_record))
        self._conn.execute(stmt, row_data)
        self._conn.commit()


class DatasetCache(RecordCache):
    def __init__(self, file_path: Optional[str], read_only: bool, dataset_type: Type[_DATASET_T]):
        self._entry_type = dataset_type._entry_type
        self._specification_type = dataset_type._specification_type
        self._record_type = dataset_type._record_type

        RecordCache.__init__(self, file_path=file_path, read_only=read_only)

    def _assert_writable(self):
        assert not self.read_only, "This dataset cache is read-only"

    def _create_tables(self):
        self._assert_writable()

        RecordCache._create_tables(self)

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )
        """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_entries (
                name TEXT PRIMARY KEY,
                entry BLOB NOT NULL
            )
        """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_specifications (
                name TEXT PRIMARY KEY,
                specification BLOB NOT NULL
            )
        """
        )

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_records (
                entry_name TEXT NOT NULL,
                specification_name TEXT NOT NULL,
                record_id INTEGER NOT NULL,
                PRIMARY KEY (entry_name, specification_name),
                FOREIGN KEY (entry_name) REFERENCES dataset_entries(name) ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (specification_name) REFERENCES dataset_specifications(name) ON DELETE CASCADE ON UPDATE CASCADE,
                FOREIGN KEY (record_id) REFERENCES records (id)
            )
        """
        )

        self._conn.execute("CREATE INDEX IF NOT EXISTS dataset_records_entry_name ON dataset_records (entry_name)")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS dataset_records_specification_name ON dataset_records (specification_name)"
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS dataset_records_record_id ON dataset_records (record_id)")

    def get_metadata(self, key) -> Any:
        stmt = "SELECT value FROM metadata WHERE key = ?"
        r = self._conn.execute(stmt, (key,)).fetchone()
        return deserialize(r[0], "msgpack")

    def entry_exists(self, name: str) -> bool:
        stmt = "SELECT 1 FROM dataset_entries WHERE name=?"
        return self._conn.execute(stmt, (name,)).fetchone() is not None

    def get_entry_names(self) -> List[str]:
        stmt = "SELECT name FROM dataset_entries"
        return [x[0] for x in self._conn.execute(stmt).fetchall()]

    def get_entry(self, name: str) -> Optional[BaseModel]:
        stmt = "SELECT entry FROM dataset_entries WHERE name=?"
        entry_data = self._conn.execute(stmt, (name,)).fetchone()
        if entry_data is None:
            return None
        return decompress_from_cache(entry_data[0], self._entry_type)

    def get_entries(self, names: Iterable[str]) -> List[BaseModel]:
        all_entries = []
        for names_batch in chunk_iterable(names, _query_chunk_size):
            name_param = ",".join("?" * len(names_batch))
            stmt = f"""SELECT entry FROM dataset_entries WHERE name IN ({name_param})"""
            entry_data = self._conn.execute(stmt, (*names_batch,)).fetchall()
            all_entries.extend(decompress_from_cache(x[0], self._entry_type) for x in entry_data)
        return all_entries

    def update_entries(self, entries: Iterable[BaseModel]):
        self._assert_writable()

        assert all(isinstance(e, self._entry_type) for e in entries)

        # TODO - update multiple at once (VALUES ((?, ?, ?, ?), (?, ?, ?, ?))
        for entry in entries:
            stmt = "REPLACE INTO dataset_entries (name, entry) VALUES (?, ?)"
            row_data = (entry.name, compress_for_cache(entry))
            self._conn.execute(stmt, row_data)

        self._conn.commit()

    def rename_entry(self, old_name: str, new_name: str):
        self._assert_writable()

        entry = self.get_entry(old_name)
        if entry is None:  # does not exist
            return

        entry.name = new_name

        stmt = "UPDATE dataset_entries SET name=?, entry=? WHERE name=?"
        self._conn.execute(stmt, (new_name, compress_for_cache(entry), old_name))
        self._conn.commit()

    def delete_entry(self, name):
        self._assert_writable()

        stmt = "DELETE FROM dataset_entries WHERE name=?"
        self._conn.execute(stmt, (name,))
        self._conn.commit()

    def specification_exists(self, name: str) -> bool:
        stmt = "SELECT 1 FROM dataset_specifications WHERE name=?"
        return self._conn.execute(stmt, (name,)).fetchone() is not None

    def get_specification_names(self) -> List[str]:
        stmt = "SELECT name FROM dataset_specifications"
        return [x[0] for x in self._conn.execute(stmt).fetchall()]

    def get_specification(self, name: str):
        stmt = "SELECT specification FROM dataset_specifications WHERE name=?"
        spec_data = self._conn.execute(stmt, (name,)).fetchone()
        if spec_data is None:
            return None
        return decompress_from_cache(spec_data[0], self._specification_type)

    def get_all_specifications(self) -> List[BaseModel]:
        stmt = "SELECT specification FROM dataset_specifications"
        spec_data = self._conn.execute(stmt).fetchall()
        return [decompress_from_cache(x[0], self._specification_type) for x in spec_data]

    def get_specifications(self, names: Iterable[str]) -> List[BaseModel]:
        name_param = ",".join("?" * len(names))

        stmt = f"""SELECT specification FROM dataset_specifications WHERE name IN ({name_param})"""
        entry_data = self._conn.execute(stmt, (*names,)).fetchall()
        return [decompress_from_cache(x[0], self._specification_type) for x in entry_data]

    def update_specifications(self, specifications: Iterable[BaseModel]):
        self._assert_writable()

        assert all(isinstance(s, self._specification_type) for s in specifications)

        # TODO - update multiple at once (VALUES ((?, ?, ?, ?), (?, ?, ?, ?))
        for specification in specifications:
            stmt = "REPLACE INTO dataset_specifications (name, specification) VALUES (?, ?)"
            row_data = (specification.name, compress_for_cache(specification))
            self._conn.execute(stmt, row_data)

        self._conn.commit()

    def rename_specification(self, old_name: str, new_name: str):
        self._assert_writable()

        specification = self.get_specification(old_name)
        if specification is None:  # does not exist
            return

        specification.name = new_name

        stmt = "UPDATE dataset_specifications SET name=?, specification=? WHERE name=?"
        self._conn.execute(stmt, (new_name, compress_for_cache(specification), old_name))
        self._conn.commit()

    def delete_specification(self, name):
        self._assert_writable()

        stmt = "DELETE FROM dataset_specifications WHERE name=?"
        self._conn.execute(stmt, (name,))
        self._conn.commit()

    def dataset_record_exists(self, entry_name: str, specification_name: str) -> bool:
        stmt = "SELECT 1 FROM dataset_records WHERE entry_name=? and specification_name=?"
        return self._conn.execute(stmt, (entry_name, specification_name)).fetchone() is not None

    def get_dataset_record(self, entry_name: str, specification_name: str):
        stmt = """SELECT r.uid, r.record FROM records r
                  INNER JOIN dataset_records dr ON r.id = dr.record_id
                  WHERE dr.entry_name=? and dr.specification_name=?"""

        record_data = self._conn.execute(stmt, (entry_name, specification_name)).fetchone()
        if record_data is None:
            return None

        record = decompress_from_cache(record_data[1], self._record_type)

        record._record_cache = self
        record._record_cache_uid = record_data[0]

        return record

    def get_dataset_records(self, entry_names: Iterable[str], specification_names: Iterable[str]):
        specification_params = ",".join("?" * len(specification_names))
        all_records = []

        for entry_names_batch in chunk_iterable(entry_names, _query_chunk_size):
            entry_params = ",".join("?" * len(entry_names_batch))

            stmt = f"""SELECT r.uid, dr.entry_name, dr.specification_name, r.record
                       FROM dataset_records dr
                       INNER JOIN records r ON r.id = dr.record_id
                       WHERE dr.entry_name IN ({entry_params})
                       AND dr.specification_name IN ({specification_params})"""

            all_params = (*entry_names_batch, *specification_names)
            rdata = self._conn.execute(stmt, all_params).fetchall()

            for uid, ename, sname, compressed_record in rdata:
                record = decompress_from_cache(compressed_record, self._record_type)

                record._record_cache = self
                record._record_cache_uid = uid

                all_records.append((ename, sname, record))

        return all_records

    def update_dataset_records(self, record_info: Iterable[Tuple[str, str, Any]]):
        self._assert_writable()

        assert all(isinstance(r, self._record_type) for _, _, r in record_info)

        # TODO - update multiple at once (VALUES ((?, ?, ?, ?), (?, ?, ?, ?))
        for entry_name, specification_name, record in record_info:
            stmt = """REPLACE INTO records (id, status, modified_on, record)
                      VALUES (?, ?, ?, ?)"""

            row_data = (
                record.id,
                record.status,
                record.modified_on.timestamp(),
                compress_for_cache(record),
            )
            self._conn.execute(stmt, row_data)

            stmt = """REPLACE INTO dataset_records (entry_name, specification_name, record_id)
                      VALUES (?, ?, ?)"""

            row_data = (
                entry_name,
                specification_name,
                record.id,
            )
            self._conn.execute(stmt, row_data)

        self._conn.commit()

    def delete_dataset_record(self, entry_name: str, specification_name: str):
        self._assert_writable()

        stmt = "DELETE FROM dataset_records WHERE entry_name=? AND specification_name=?"
        self._conn.execute(stmt, (entry_name, specification_name))
        self._conn.commit()

    def delete_dataset_records(
        self, entry_names: Optional[Iterable[str]], specification_names: Optional[Iterable[str]]
    ):
        self._assert_writable()

        all_params = []
        conds = []

        stmt = f"DELETE FROM dataset_records "

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

    def get_dataset_record_info(
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

            stmt = f"""SELECT dr.entry_name, dr.specification_name, dr.record_id, r.status, r.modified_on
                       FROM dataset_records dr
                       INNER JOIN records r ON r.id = dr.record_id
                       WHERE dr.entry_name IN ({entry_params})
                       AND dr.specification_name IN ({specification_params})"""

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

    def get_existing_dataset_records(
        self, entry_names: Iterable[str], specification_names: Iterable[str]
    ) -> List[Tuple[str, str, int]]:
        specification_params = ",".join("?" * len(specification_names))

        ret = []
        for entry_names_batch in chunk_iterable(entry_names, _query_chunk_size):
            entry_params = ",".join("?" * len(entry_names_batch))

            stmt = f"""SELECT entry_name, specification_name, record_id
                       FROM dataset_records
                       WHERE entry_name IN ({entry_params})
                       AND specification_name IN ({specification_params})"""

            r = self._conn.execute(stmt, (*entry_names_batch, *specification_names)).fetchall()
            ret.extend(r)

        return ret


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
    r = conn.execute("SELECT value FROM metadata WHERE key = 'dataset_metadata'")
    if r is None:
        raise RuntimeError(f"Cannot find appropriate metadata in cache file {file_path}")

    d = deserialize(r.fetchone()[0], "msgpack")
    conn.close()
    return d
