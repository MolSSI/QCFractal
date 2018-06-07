"""
Queue backend abstraction manager.
"""


def build_queue(queue_type, queue_socket, db_socket, **kwargs):

    if queue_type == "dask":
        try:
            import dask.distributed
        except ImportError:
            raise ImportError("Dask.distributed not installed, please install dask.distributed for the dask queue client.")

        from . import dask_handler

        nanny = dask_handler.DaskNanny(queue_socket, db_socket, **kwargs)
        scheduler = dask_handler.DaskScheduler

    elif queue_type == "fireworks":
        try:
            import fireworks
        except ImportError:
            raise ImportError("Fireworks not installed, please install fireworks for the fireworks queue client.")

        from . import fireworks_handler

        nanny = fireworks_handler.FireworksNanny(queue_socket, db_socket, **kwargs)
        scheduler = fireworks_handler.FireworksScheduler

    else:
        raise KeyError("Queue type '{}' not understood".format(queue_type))

    return (nanny, scheduler)
