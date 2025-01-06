"""
Caching for the PortalClient
"""

from __future__ import annotations

import datetime
import os
from typing import TYPE_CHECKING, Optional, TypeVar, Type, Any, List, Iterable, Tuple, Sequence
from urllib.parse import urlparse

import apsw

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
    from qcportal.client import PortalClient

_DATASET_T = TypeVar("_DATASET_T")
_RECORD_T = TypeVar("_RECORD_T")

_query_chunk_size = 125


def compress_for_cache(data: Any) -> bytes:
    serialized_data = serialize(data, "msgpack")
    compressed_data = zstandard.compress(serialized_data, level=1)
    return compressed_data


def decompress_from_cache(data: bytes, value_type) -> Any:
    decompressed_data = zstandard.decompress(data)
    deserialized_data = deserialize(decompressed_data, "msgpack")
    return pydantic.parse_obj_as(value_type, deserialized_data)


class RecordCache:
    def __init__(self, cache_uri: str, read_only: bool):
        self.cache_uri = cache_uri
        self.read_only = read_only

        if self.read_only:
            self._conn = apsw.Connection(self.cache_uri, flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI)
        else:
            self._conn = apsw.Connection(
                self.cache_uri, flags=apsw.SQLITE_OPEN_READWRITE | apsw.SQLITE_OPEN_CREATE | apsw.SQLITE_OPEN_URI
            )

        self._conn.pragma("foreign_keys", "ON")

        if not read_only:
            self._create_tables()

    def __str__(self):
        return f"<{self.__class__.__name__} path={self.cache_uri} {'ro' if self.read_only else 'rw'}>"

    def _assert_writable(self):
        assert not self.read_only, "This cache is read-only"

    def _create_tables(self):
        self._assert_writable()

        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                modified_on DECIMAL NOT NULL,
                record BLOB NOT NULL
            )
            """
        )

        self._conn.execute("CREATE INDEX IF NOT EXISTS records_status ON records (status)")

    def update_metadata(self, key: str, value: Any) -> None:
        self._assert_writable()
        stmt = "REPLACE INTO metadata (key, value) VALUES (?, ?)"
        self._conn.execute(stmt, (key, serialize(value, "msgpack")))

    def get_record(self, record_id: int, record_type: Type[_RECORD_T]) -> Optional[_RECORD_T]:
        stmt = "SELECT record FROM records WHERE id = ?"

        record_data = self._conn.execute(stmt, (record_id,)).fetchone()
        if record_data is None:
            return None

        record = decompress_from_cache(record_data[1], record_type)

        record._record_cache = self

        return record

    def get_records(self, record_ids: Iterable[int], record_type: Type[_RECORD_T]) -> List[_RECORD_T]:
        all_records = []

        for record_id_batch in chunk_iterable(record_ids, _query_chunk_size):
            id_params = ",".join("?" * len(record_id_batch))
            stmt = f"SELECT record FROM records WHERE id IN ({id_params})"

            rdata = self._conn.execute(stmt, record_id_batch).fetchall()

            for compressed_record in rdata:
                record = decompress_from_cache(compressed_record[0], record_type)

                record._record_cache = self

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
        self._assert_writable()

        with self._conn:
            for record_batch in chunk_iterable(records, 10):
                n_batch = len(record_batch)

                values_params = ",".join(["(?, ?, ?, ?)"] * n_batch)

                all_params = []
                for r in record_batch:
                    all_params.extend((r.id, r.status, r.modified_on.timestamp(), compress_for_cache(r)))

                stmt = f"REPLACE INTO records (id, status, modified_on, record) VALUES {values_params}"

                self._conn.execute(stmt, all_params)

        for r in records:
            r._record_cache = self
            r._cache_dirty = False

    def writeback_record(self, record):
        self._assert_writable()

        compressed_record = compress_for_cache(record)

        # Only update if timestamp is same or newer, and if this record is larger
        # than what is stored already
        stmt = f"""INSERT OR REPLACE INTO records (id, status, modified_on, record)
                   SELECT ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM records WHERE id = ?
                   AND (modified_on > ? OR (modified_on = ? and length(record) > ?)))"""

        ts = record.modified_on.timestamp()
        row_data = (record.id, record.status, ts, compressed_record, record.id, ts, ts, len(compressed_record))
        self._conn.execute(stmt, row_data)

    def delete_record(self, record_id: int):
        self._assert_writable()

        stmt = "DELETE FROM records WHERE id=?"
        self._conn.execute(stmt, (record_id,))

    def delete_records(self, record_ids: Iterable[int]):
        self._assert_writable()

        for record_id_batch in chunk_iterable(record_ids, _query_chunk_size):
            record_id_params = ",".join("?" * len(record_id_batch))
            stmt = f"DELETE FROM records WHERE id IN ({record_id_params})"
            self._conn.execute(stmt, record_id_batch)


class DatasetCache(RecordCache):
    def __init__(self, cache_uri: str, read_only: bool, dataset_type: Type[_DATASET_T]):
        self._entry_type = dataset_type._entry_type
        self._specification_type = dataset_type._specification_type
        self._record_type = dataset_type._record_type

        RecordCache.__init__(self, cache_uri=cache_uri, read_only=read_only)

    def _create_tables(self):
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
                FOREIGN KEY (specification_name) REFERENCES dataset_specifications(name) ON DELETE CASCADE ON UPDATE CASCADE
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

        with self._conn:
            for entry_batch in chunk_iterable(entries, 50):
                n_batch = len(entry_batch)
                values_params = ",".join(["(?, ?)"] * n_batch)

                all_params = []
                for e in entry_batch:
                    all_params.extend((e.name, compress_for_cache(e)))

                stmt = f"REPLACE INTO dataset_entries (name, entry) VALUES {values_params}"
                self._conn.execute(stmt, all_params)

    def rename_entry(self, old_name: str, new_name: str):
        self._assert_writable()

        entry = self.get_entry(old_name)
        if entry is None:  # does not exist
            return

        entry.name = new_name

        stmt = "UPDATE dataset_entries SET name=?, entry=? WHERE name=?"
        self._conn.execute(stmt, (new_name, compress_for_cache(entry), old_name))

    def delete_entry(self, name):
        self._assert_writable()

        stmt = "DELETE FROM dataset_entries WHERE name=?"
        self._conn.execute(stmt, (name,))

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

        with self._conn:
            for specification_batch in chunk_iterable(specifications, 50):
                n_batch = len(specification_batch)
                values_params = ",".join(["(?, ?)"] * n_batch)

                all_params = []
                for s in specification_batch:
                    all_params.extend((s.name, compress_for_cache(s)))

                stmt = f"REPLACE INTO dataset_specifications (name, specification) VALUES {values_params}"
                self._conn.execute(stmt, all_params)

    def rename_specification(self, old_name: str, new_name: str):
        self._assert_writable()

        specification = self.get_specification(old_name)
        if specification is None:  # does not exist
            return

        specification.name = new_name

        stmt = "UPDATE dataset_specifications SET name=?, specification=? WHERE name=?"
        self._conn.execute(stmt, (new_name, compress_for_cache(specification), old_name))

    def delete_specification(self, name):
        self._assert_writable()

        stmt = "DELETE FROM dataset_specifications WHERE name=?"
        self._conn.execute(stmt, (name,))

    def dataset_record_exists(self, entry_name: str, specification_name: str) -> bool:
        stmt = "SELECT 1 FROM dataset_records WHERE entry_name=? and specification_name=?"
        return self._conn.execute(stmt, (entry_name, specification_name)).fetchone() is not None

    def get_dataset_record(self, entry_name: str, specification_name: str) -> Optional[_RECORD_T]:
        stmt = """SELECT r.record FROM records r
                  INNER JOIN dataset_records dr ON r.id = dr.record_id
                  WHERE dr.entry_name=? and dr.specification_name=?"""

        record_data = self._conn.execute(stmt, (entry_name, specification_name)).fetchone()
        if record_data is None:
            return None

        record = decompress_from_cache(record_data[0], self._record_type)
        record._record_cache = self

        return record

    def get_dataset_records(
        self,
        entry_names: Iterable[str],
        specification_names: Iterable[str],
        status: Optional[Iterable[RecordStatusEnum]] = None,
    ) -> List[Tuple[str, str, _RECORD_T]]:
        specification_params = ",".join("?" * len(specification_names))
        all_records = []

        for entry_names_batch in chunk_iterable(entry_names, _query_chunk_size):
            entry_params = ",".join("?" * len(entry_names_batch))

            stmt = f"""SELECT dr.entry_name, dr.specification_name, r.record
                       FROM dataset_records dr
                       INNER JOIN records r ON r.id = dr.record_id
                       WHERE dr.entry_name IN ({entry_params})
                       AND dr.specification_name IN ({specification_params})"""

            all_params = (*entry_names_batch, *specification_names)

            if status:
                status_params = ",".join("?" * len(status))
                stmt = stmt + f"AND r.status IN ({status_params})"
                all_params = (*all_params, *status)

            rdata = self._conn.execute(stmt, all_params).fetchall()

            for ename, sname, compressed_record in rdata:
                record = decompress_from_cache(compressed_record, self._record_type)
                record._record_cache = self

                all_records.append((ename, sname, record))

        return all_records

    def update_dataset_records(self, record_info: Iterable[Tuple[str, str, int]]):
        self._assert_writable()

        with self._conn:
            for info_batch in chunk_iterable(record_info, 10):
                n_batch = len(info_batch)
                values_params = ",".join(["(?, ?, ?)"] * n_batch)

                all_params = []
                for e, s, rid in info_batch:
                    all_params.extend((e, s, rid))

                stmt = f"""REPLACE INTO dataset_records (entry_name, specification_name, record_id)
                          VALUES {values_params}"""
                self._conn.execute(stmt, all_params)

    def delete_dataset_record(self, entry_name: str, specification_name: str):
        self._assert_writable()

        stmt = "DELETE FROM dataset_records WHERE entry_name=? AND specification_name=?"
        self._conn.execute(stmt, (entry_name, specification_name))

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
            # _shared_memory shouldn't be used, so we don't set it and wait for errors
            self._is_disk = True
            self.cache_dir = os.path.join(os.path.abspath(cache_dir), self.server_fingerprint)
            os.makedirs(self.cache_dir, exist_ok=True)
        else:
            self._is_disk = False

            self.cache_dir = None

    def get_cache_path(self, cache_name: str) -> str:
        if not self._is_disk:
            raise RuntimeError("Cannot get path to cache for memory-only cache")

        return os.path.join(self.cache_dir, f"{cache_name}.sqlite")

    def get_cache_uri(self, cache_name: str) -> str:
        if self._is_disk:
            file_path = self.get_cache_path(cache_name)
            uri = f"file:{file_path}"
        else:
            uri = ":memory:"

        return uri

    def get_dataset_cache_path(self, dataset_id: int) -> str:
        return self.get_cache_path(f"dataset_{dataset_id}")

    def get_dataset_cache_uri(self, dataset_id: int) -> str:
        return self.get_cache_uri(f"dataset_{dataset_id}")

    def get_dataset_cache(self, dataset_id: int, dataset_type: Type[_DATASET_T]) -> DatasetCache:
        uri = self.get_dataset_cache_uri(dataset_id)

        # If you are asking this for a dataset cache, it should be writable
        return DatasetCache(uri, False, dataset_type)

    @property
    def is_disk(self) -> bool:
        return self._is_disk

    def vacuum(self, cache_name: Optional[str] = None):
        if self._is_disk:
            # TODO
            return


def read_dataset_metadata(file_path: str):
    """
    Reads the type of dataset stored in a cache file

    This is needed sometimes to construct the DatasetCache object with a type
    """

    file_path = os.path.abspath(file_path)

    if not os.path.isfile(file_path):
        raise RuntimeError(f'Cannot open cache file "{file_path}" - does not exist or is not a file')

    uri = f"file:{file_path}?mode=ro"
    conn = apsw.Connection(uri, flags=apsw.SQLITE_OPEN_READONLY | apsw.SQLITE_OPEN_URI)

    r = conn.execute("SELECT value FROM metadata WHERE key = 'dataset_metadata'")
    if r is None:
        raise RuntimeError(f"Cannot find appropriate metadata in cache file {file_path}")

    d = deserialize(r.fetchone()[0], "msgpack")
    conn.close()
    return d


def get_records_with_cache(
    client: Optional[PortalClient],
    record_cache: Optional[RecordCache],
    record_type: Type[_RECORD_T],
    record_ids: Sequence[int],
    include: Optional[Iterable[str]] = None,
    force_fetch: bool = False,
) -> List[_RECORD_T]:
    """
    Helper function for obtaining child records either from the cache or from the server

    The records are returned in the same order as the `record_ids` parameter.

    If records are missing from the cache, and client is None, and exception is raised.

    Newly-fetched records will not be immediately written to the cache. Instead, they will be attached to this cache
    and will be written back to the cache when the record object is destructed.

    If `include` is specified, additional fields will be fetched from the server. However, if the records are in the
    cache already, they may be missing those fields. In that case, the additional information may be fetched
    from the server. If a client is not provided, an exception will be raised.

    This function will fetch the children of the records if enough information
    is fetched of the parent record. This is handled by the various fetch_children_multi
    class functions of the record types.


    Parameters
    ----------
    client
        The client to use for fetching records from the server.
        If `None`, the function will only use the cache
    record_cache
        The cache to use for fetching records from the cache.
        If `None`, the function will only use the client
    record_type
        The type of record to fetch
    record_ids
        Single ID or sequence/list of records to obtain
    include
        Additional fields to include in the returned record (if fetching from the client)
    force_fetch
        If `True`, the function will fetch all records from the server,
        regardless of whether they are in the cache

    Returns
    -------
    :
        List of records in the same order as the input `record_ids`.
    """

    if record_cache is None or force_fetch:
        existing_records = []
        records_tofetch = set(record_ids)
    else:
        existing_records = record_cache.get_records(record_ids, record_type)
        records_tofetch = set(record_ids) - {x.id for x in existing_records}

        for r in existing_records:
            r.propagate_client(client)

    if records_tofetch:
        if client is None:
            raise RuntimeError("Need to fetch some records, but not connected to a client")

        recs = client._fetch_records(record_type, list(records_tofetch), include=include)

        # Set up for the writeback on change, but write the record as-is for now
        if record_cache is not None:
            record_cache.update_records(recs)
        for r in recs:
            r._record_cache = record_cache
            r._cache_dirty = False

        existing_records += recs

    # Fetch all children as well
    record_type.fetch_children_multi(existing_records, include=include, force_fetch=force_fetch)

    # Return everything in the same order as the input
    all_recs = {r.id: r for r in existing_records}
    ret = [all_recs.get(rid, None) for rid in record_ids]

    if any(x is None for x in ret):
        missing_ids = set(record_ids) - set(all_recs.keys())
        raise RuntimeError(
            f"Not all records found either in the cache or on the server. Missing records: {missing_ids}"
        )

    return ret
