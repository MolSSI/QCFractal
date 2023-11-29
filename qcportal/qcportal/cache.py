"""PortalClient cache interface.

"""

import dbm
import json
import lzma
import os
import time
from typing import List, Dict, Any

from qcelemental.models import Molecule


# from .records_ddotson import record_factory
# from .collections.collection_utils import collection_factory


class PortalCache:
    def __init__(self, client, cachedir, max_memcache_size):
        self.client = client

        # TODO: need server fingerprint of some kind to use for cachedir
        # must resolve to same value for same server on each use, but to different value for different server
        self.server_fingerprint = self.client.server_name.replace(" ", "_")
        if cachedir:
            self.cachedir = os.path.join(os.path.abspath(cachedir), self.server_fingerprint)
            self.metafile = os.path.join(self.cachedir, "meta.json")
            self.cachefile = os.path.join(self.cachedir, "cache.db")
            os.makedirs(self.cachedir, exist_ok=True)

            # initialize db if it doesn't exist
            with dbm.open(self.cachefile, "c") as db:
                pass

            # writeout metadata for reload later, exception handling if server/cache mismatch
            if not os.path.exists(self.metafile):
                self.stamp_cache()
            else:
                self.check_cache()

        else:
            self.cachedir = None
            self.metafile = None
            self.cachefile = None

        self.memcache = MemCache(maxsize=max_memcache_size)

    def put(self, items: List[Any], entity_type: str):
        """Put a list of items into the cache.

        See `entity_type` for allowed values.

        Parameters
        ----------
        items : List[Any]
            List of objects to cache.
        entity_type : str
            Type of entity; one of `molecule`, `record`.

        """
        if entity_type not in ("molecule", "record"):
            raise ValueError("`entity_type` must be one of `molecule`, `record`")

        if self.cachefile:
            with dbm.open(self.cachefile, "c") as db:
                self._put(items, entity_type, db)
        else:
            self._put(items, entity_type)

    def _put(self, items, entity_type, db=None):
        for item in items:
            self._put_single(item, entity_type, db)

    def _put_single(self, item, entity_type, db=None):
        key = f"{entity_type}-{item.id}"

        # if we already have this in memcache, no further action
        if key in self.memcache:
            return

        # add to memcache
        self.memcache[key] = item

        # add to fs cache
        if db is not None:
            db[key] = lzma.compress(item.json().encode("utf-8"))

    def get(self, ids: List[str], entity_type: str) -> Dict[str, Any]:
        """Get a list of items out of the cache.

        See `entity_type` for allowed values.

        Parameters
        ----------
        ids: List[str]
            List of objects to retrieve from cache.
        entity_type : str
            Type of entity; one of `molecule`, `record`.

        Returns
        -------

        """
        if entity_type not in ("molecule", "record"):
            raise ValueError("`entity_type` must be one of `molecule`, `record`")

        if self.cachefile:
            with dbm.open(self.cachefile, "r") as db:
                return self._get(ids, entity_type, db)
        else:
            return self._get(ids, entity_type)

    def _get(self, ids, entity_type, db=None):
        items = {}
        for i in ids:
            item = self._get_single(str(i), entity_type, db)
            if item is not None:
                items[i] = item
        return items

    def _get_single(self, id, entity_type, db=None):
        key = f"{entity_type}-{id}"

        # first check memcache (fast)
        # if found, return
        item = self.memcache.get(key, None)
        if item is not None:
            return item

        # check fs cache (slower)
        # return if found, otherwise return None
        if db is not None:
            item = db.get(key, None)
            if item is not None:
                if entity_type == "molecule":
                    item = Molecule.from_data(json.loads(lzma.decompress(item).decode()))

                elif entity_type == "record":
                    item = record_factory(json.loads(lzma.decompress(item).decode()), client=self.client)

                # add to memcache
                self.memcache[key] = item

                return item
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


# TODO: consider making this a dict subclass for performance instead of composition
class MemCache:
    def __init__(self, maxsize):
        self.data = {}
        self.maxsize = maxsize

    def __setitem__(self, key, value):
        # check size; if we're beyond, chop least-recently-used value
        self.garbage_collect()
        self.data[key] = {"value": value, "last_used": time.time()}

    def __getitem__(self, key):
        # update last_used, then return
        # TODO: perhaps a more performant way to do this
        result = self.data[key]
        result["last_used"] = time.time()

        return result["value"]

    def __contains__(self, item):
        return item in self.data

    def garbage_collect(self):
        # if cache is beyond max size, whittle it down by dropping
        # the 3/4 of it; a bit aggressive but avoids constaint thrashing
        # TODO: efficiency gains perhaps achievable here
        if (self.maxsize is not None) and len(self.data) > self.maxsize:
            newsize = self.maxsize // 4
            items = sorted(self.data.items(), key=lambda x: x[1]["last_used"])
            remove = items[:-newsize]
            for key, value in remove:
                self.data.pop(key)

    def get(self, key, default=None):
        return self[key] if key in self else default
