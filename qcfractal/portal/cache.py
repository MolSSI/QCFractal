"""PortalClient cache interface.

"""


class PortalCache:
    def __init__(self, client, cachedir):
        self.client = client
        self.cachedir = cachedir
