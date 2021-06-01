"""PortalClient cache interface.

"""

import os
import json
import bz2
import dbm
import functools

from typing import Union, List, Dict

from .records import record_factory
from .collections.collection_utils import collection_factory


# TODO: make caching for different servers a layer beneath, ultimately transparent for most users
# TODO: consider a single file for serialized JSON caching, key-value store to avoid FS thrashing
class PortalCache:
    def __init__(self, client, cachedir):
        self.client = client

        # TODO: need server fingerprint of some kind to use for cachedir
        # must resolve to same value for same server on each use, but to different value for different server
        self.server_fingerprint = self.client.server_name.replace(" ", "_")
        if cachedir:
            self.cachedir = os.path.join(os.path.abspath(cachedir), self.server_fingerprint)
            self.metafile = os.path.join(self.cachedir, "meta.json")
            self.cachefile = os.path.join(self.cachedir, "cache.db")
            os.makedirs(self.cachedir, exist_ok=True)

            # writeout metadata for reload later, exception handling if server/cache mismatch
            if not os.path.exists(self.metafile):
                self.stamp_cache()
            else:
                self.check_cache()

        else:
            self.cachedir = None
            self.metafile = None
            self.cachefile = None

        # TODO: make this an LRU cache with finite size
        # self.memcache = {}

    # def _get_writelock(self):
    #    """Context manager for applying cross-platform filesystem lock to cache lockfile.

    #    Only required for write to cache.
    #    Allows multiple clients to write without further coordination.

    #    """
    #    pass

    # def _get_readlock(self):
    #    """Context manager for applying cross-platform filesystem lock to cache lockfile.

    #    Only required for write to cache.
    #    Allows multiple clients to write without further coordination.

    #    """

    def put(self, records: Union[List[Dict], Dict]):
        if isinstance(records, list):
            for rec in records:
                self._put(rec)
        elif rec is None:
            return
        else:
            self._put(rec)

    def _put(self, record):

        # remove entr

        if isinstance(record, dict):
            id = record["id"]
        else:
            id = record.id

        ## if we already have this in memcache, no further action
        # if id in self.memcache:
        #    return

        # add to memcache
        # self.memcache[id] = record

        # add to fs cache
        if self.cachefile:
            with dbm.open(self.cachefile, "c") as db:
                db[id] = bz2.compress(record.to_json().encode("utf-8"))

    def get(self, id: Union[List[str], str]) -> Dict[str, "Record"]:
        if isinstance(id, list):
            records = {}
            for i in id:
                rec = self._get(str(i))
                if rec is not None:
                    records[i] = rec
            return records
        elif id is None:
            return {}
        else:
            self.get([str(id)])

    # NOTE: use of `lru_cache` here creates a situation where a result may not be in the memory cache
    # but could be added to the file cache by the same or another process
    # the lru cache would continue returning `None`, skipping the cache entirely
    @functools.lru_cache(max_size=1000)
    def _get(self, id):
        # first check memcache (fast)
        # if found, return
        # record = self.memcache.get(id, None)
        # if record is not None:
        #    return record

        # check fs cache (slower)
        # return if found, otherwise return None
        if self.cachefile:
            with dbm.open(self.cachefile, "r") as db:
                record = db.get(id, None)
                if record is not None:
                    return record_factory(json.loads(bz2.decompress(record).decode()))
                else:
                    return

    def stamp_cache(self):
        """Place metadata indicating which server this cache belongs to."""
        meta = {"purpose": "QCFractal PortalClient cache", "server": self.client.address}

        with open(self.metafile, "w") as f:
            json.dump(meta, f)

    def check_cache(self):
        with open(self.metafile, "r") as f:
            meta = json.load(f)

            # TODO: consider other ways to verify same server besides URI
            # is there some kind of fingerprint the server keeps for itself?
            if meta["server"] != self.client.address:
                raise Exception("Existing cache directory corresponds to a different QCFractal Server")
