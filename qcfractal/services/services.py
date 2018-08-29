"""
Maniuplates available services.
"""


def initializer(name, db_socket, queue_socket, meta, molecule):

    name = name.lower()
    if name == "torsiondrive":
        from .torsiondrive_service import TorsionDriveService

        return TorsionDriveService.initialize_from_api(db_socket, queue_socket, meta, molecule)
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))


def build(name, db_socket, queue_socket, data):
    name = name.lower()
    if name == "torsiondrive":
        from .torsiondrive_service import TorsionDriveService

        return TorsionDriveService(db_socket, queue_socket, data)
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))
