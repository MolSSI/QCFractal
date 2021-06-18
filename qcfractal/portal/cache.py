"""PortalClient cache interface.

"""

import os
import json
import bz2
import dbm
import time

from typing import Union, List, Dict

from .records import record_factory
from .collections.collection_utils import collection_factory



# TODO: consider lzma, zstd compression instead of bz2 for faster read at the cost of perhaps slower write
class PortalCache:
    def __init__(self, client, cachedir, max_memcache_size):
        self.client = client
        
        # TODO: need server fingerprint of some kind to use for cachedir
        # must resolve to same value for same server on each use, but to different value for different server
        self.server_fingerprint = self.client.server_name.replace(' ', '_')
        if cachedir:
            self.cachedir = os.path.join(os.path.abspath(cachedir), self.server_fingerprint)
            self.metafile = os.path.join(self.cachedir, "meta.json")
            self.cachefile = os.path.join(self.cachedir, "cache.db")
            os.makedirs(self.cachedir, exist_ok=True)

            # initialize db if it doesn't exist
            with dbm.open(self.cachefile, 'c') as db:
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

    def put(self, record: Union[List[Dict], Dict]):
        if self.cachefile:
            with dbm.open(self.cachefile, 'c') as db:
                self._put(record, db)
        else:
            self._put(record)

    def _put(self, record, db=None):
        if isinstance(record, list):
            for rec in record:
                self._put_single(rec, db)
        elif record is None:
            return
        else:
            self._put_single(record, db)

    def _put_single(self, record, db=None):
        id = record.id

        # if we already have this in memcache, no further action
        if id in self.memcache:
            return

        # add to memcache
        self.memcache[id] = record

        # add to fs cache
        if db is not None:
            db[id] = bz2.compress(record.to_json().encode("utf-8"))

    def get(self, id: Union[List[str], str]) -> Dict[str, "Record"]:
        if self.cachefile:
            with dbm.open(self.cachefile, 'r') as db:
                return self._get(id, db)
        else:
            return self._get(id)

    def _get(self, id, db=None):
        if isinstance(id, list):
            records = {}
            for i in id:
                rec = self._get_single(str(i), db)
                if rec is not None:
                    records[i] = rec
            return records
        elif id is None:
            return {}
        else:
            return {id: self._get_single(str(id), db)}

    def _get_single(self, id, db=None):
        # first check memcache (fast)
        # if found, return
        record = self.memcache.get(id, None)
        if record is not None:
            return record

        # check fs cache (slower)
        # return if found, otherwise return None
        if db is not None:
            record = db.get(id, None)
            if record is not None:
                rec = record_factory(json.loads(bz2.decompress(record).decode()), client=self.client)

                # add to memcache
                self.memcache[id] = rec

                return rec
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

        self.garbage_collect()
        
        # check size; if we're beyond, chop least-recently-used value
        self.data[key] = {'value': value, 'last_used': time.time()}

    def __getitem__(self, key):
        # update last_used, then return
        # TODO: perhaps a more performant way to do this
        result = self.data[key]
        result['last_used'] = time.time()

        return result['value']

    def __contains__(self, item):
        return item in self.data
        
    def garbage_collect(self):
        # if cache is beyond max size, whittle it down by dropping
        # the 3/4 of it; a bit aggressive but avoids constaint thrashing
        # TODO: efficiency gains perhaps achievable here
        if (self.maxsize is not None) and len(self.data) > self.maxsize:
            newsize = self.maxsize//4
            items = sorted(self.data.items(), key=lambda x: x[1]['last_used'])
            remove = items[:-newsize]
            for (key, value) in remove:
                self.data.pop(key)

    def get(self, key, default=None):
        return self[key] if key in self else default
