"""PortalClient cache interface.

"""

import os
import json
import bz2

from typing import Union, List, Dict

from ..interface.models.records import RecordBase
from ..interface.models import build_procedure 
from .collections.collection_utils import collection_factory


# TODO: make caching for different servers a layer beneath, ultimately transparent for most users
class PortalCache:

    def __init__(self, client, cachedir):
        self.client = client
        self.cachedir = os.path.abspath(cachedir)
        self.metafile = os.path.join(self.cachedir, 'meta.json')

        os.makedirs(self.cachedir, exist_ok=True)

        # writeout metadata for reload later, exception handling if server/cache mismatch
        if not os.path.exists(self.metafile):
            self.stamp_cache()
        else:
            self.check_cache()

        # TODO: make this an LRU cache with finite size
        self.memcache = {}

    def _get_writelock(self):
        """Context manager for applying cross-platform filesystem lock to cache lockfile.
        
        Only required for write to cache.
        Allows multiple clients to write without further coordination.

        """
        pass

    def put(self, records: Union[List[Dict], Dict]):
        if isinstance(records, list):
            for rec in records:
                self._put(rec)
        elif rec is None:
            return
        else:
            self._put(rec)
                
    def _put(self, record):

        if isinstance(record, dict):
            id = record['id']
        else:
            id = record.id

        # if we already have this in memcache, no further action
        if id in self.memcache:
            return
        
        # add to memcache
        self.memcache[id] = record

        # add to fs cache
        cachefile = os.path.join(self.cachedir, "{}.json.bz2".format(id))
        with open(cachefile, 'wb') as f:
            f.write(bz2.compress(record.json().encode('utf-8')))

    def get(self, ids: Union[List[str], str]):
        if isinstance(ids, list):
            records = {}
            for id in ids:
                rec = self._get(id)
                if rec is not None:
                    records[id] = rec
            return records
        elif id is None:
            return []
        else:
            return self._get(id)

    def _get(self, id):

        # cast to string if not already
        id = str(id)

        # first check memcache (fast)
        # if found, return
        record = self.memcache.get(id, None)
        if record is not None:
            return record

        # check fs cache (slower)
        # return if found, otherwise return None
        cachefile = os.path.join(self.cachedir, "{}.json.bz2".format(id))
        if os.path.exists(cachefile):
            with open(cachefile, 'rb') as f:
                return build_procedure(json.loads(bz2.decompress(f.read()).decode()))
                #return build_procedure(json.load(f))
        else:
            return




    def stamp_cache(self):
        """Place metadata indicating which server this cache belongs to.

        """
        meta = {'purpose': "QCFractal PortalClient cache",
                'server': self.client.address}

        with open(self.metafile, 'w') as f:
            json.dump(meta, f)

    def check_cache(self):
        with open(self.metafile, 'r') as f:
            meta = json.load(f)

            # TODO: consider other ways to verify same server besides URI
            # is there some kind of fingerprint the server keeps for itself?
            if meta['server'] != self.client.address:
                raise Exception("Existing cache directory corresponds to a different QCFractal Server")


