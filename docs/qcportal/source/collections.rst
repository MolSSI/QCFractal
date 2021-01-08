Overview
========

Collections are organizational objects that keep track of or compute new results, 
and provide helper functions for analysis and visualization.


Collections querying
---------------------

Once a :class:`FractalClient <qcportal.FractalClient>` has been created, it can query a list of all
collections currently held on the server.

.. code-block:: python

    >>> client.list_collections()
    {"ReactionDataset": ["S22"]}

A collection can then be pulled from the server as follows:

.. code-block:: python

    >>> client.get_collection("ReactionDataset", "S22")
    Dataset(id=`5b7f1fd57b87872d2c5d0a6d`, name=`S22`, client="localhost:7777")

Available collections
---------------------

Below is a complete list of collection types available from QCPortal.
All collections support the possibility of computing with and comparing multiple methods.

* :doc:`collection-dataset` - A collection of molecular sets and their computed properties.
* :doc:`collection-reactiondataset` - A collection of chemical reactions and intermolecular interactions.
* :doc:`collection-optimization` - A collection of geometry optimization results for molecular sets.
* :doc:`collection-torsiondrive` - A collection of TorsionDrive pipeline data.

