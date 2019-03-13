Collections
===========

Collections are an organizational objects that keeps track of collections of results
results, provides the ability to analyize and visualize these results, and is
able to compute new results.


Collections Querying
---------------------

Once a FractalClient has been created the client can query a list of all
collections currently held on the server.

.. code-block:: python

    >>> client.list_collections()
    {"dataset": ["S22"]}

A collection can then be pulled from the server as follows:

.. code-block:: python

    >>> client.get_collection("dataset", "S22")
    Dataset(id=`5b7f1fd57b87872d2c5d0a6d`, name=`S22`, client="localhost:8888")

Available Collections
---------------------

Below is a complete list of collections available from QCPortal:

* :doc:`collection-dataset` - A collection for running a single set of reactions under many methods.

