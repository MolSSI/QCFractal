Portal Collections
==================

Collections are an organizational objects that keeps track of collections of results
results, provides the ability to analyize and visualize these results, and is
able to compute new results.


Dataset Querying
----------------

.. code-block:: python

    >>> import portal as ptl
    >>> client = ptl.FractalClient("localhost:8888")
    >>> client
    FractalClient(server='http://localhost:8888/', username='None')

    >>> ptl.collections.Dataset.from_server(client, "S22")




Below is a complete list of collections available from QCPortal:

.. toctree::
   :maxdepth: 1
   :caption: Portal Collections

   collection-dataset
