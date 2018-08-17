"""
Maniuplates available services.
"""

def initializer(name, db_socket, queue_socket, meta, molecule):

    name = name.lower()
    if name == "crank":
        from .crank_service import Crank

        return Crank.initialize_from_api(db_socket, queue_socket, meta, molecule)
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))

def build(name, db_socket, queue_socket, data):
    name = name.lower()
    if name == "crank":
        from .crank_service import Crank

        return Crank(db_socket, queue_socket, data)
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))
