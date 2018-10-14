"""
Queue backend abstraction manager.
"""

from . import dask_adapter
from . import fireworks_adapter


def build_queue_adapter(workflow_client, logger=None, **kwargs):
    """Constructs a queue manager based off the incoming queue socket type.

    Parameters
    ----------
    workflow_client : object ("distributed.Client", "fireworks.LaunchPad")
        A object wrapper for different distributed workflow types
    logger : logging.Logger, Optional. Default: None
        Logger to report to
    **kwargs
        Additional kwargs for the Adapter

    Returns
    -------
    ret : Adapter
        Returns a valid Adapter for the selected computational queue
    """

    adapter_type = type(workflow_client).__module__ + "." + type(workflow_client).__name__

    if adapter_type == "distributed.client.Client":
        adapter = dask_adapter.DaskAdapter(workflow_client)

    elif adapter_type == "fireworks.core.launchpad.LaunchPad":
        adapter = fireworks_adapter.FireworksAdapter(workflow_client)

    else:
        raise KeyError("Queue type '{}' not understood".format(adapter_type))

    return adapter
