"""
Maniuplates available services.
"""

def builder(name, db_socket, queue_socket, meta, molecule):

    name = name.lower()
    if name == "crank":
        from .crank import Crank

        return Crank.initialize_from_api(db_socket, queue_socket, meta, molecule) 
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))
    
