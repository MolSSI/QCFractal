TorsionDrive Dataset
====================
:class:`TorsionDriveDatasets <qcfractal.interface.collections.TorsionDriveDataset>`
host the results of TorsionDrive computations. Each row of the
:class:`TorsionDriveDataset <qcfractal.interface.collections.TorsionDriveDataset>` 
is comprised of an ``Entry`` which contains a list of starting molecules for the
``TorsionDrive``, a specific set of dihedral angles (zero-indexed),
and the angular scan resolution. Each column represents a particular property detail 
pertinent to the corresponding ``TorsionDrive`` ``Entry``.
The :class:`TorsionDriveDataset <qcfractal.interface.collections.TorsionDriveDataset>` 
is a procedure-style dataset within which, the :term:`ObjectId` for
each ``TorsionDrive`` computation is recorded as metadata.

For additional details about :class:`TorsionDriveDatasets <qcfractal.interface.collections.TorsionDriveDataset>`
see `here <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/torsiondrive_datasets.html>`_.

Querying the Data
-----------------

Statistics and Visualization
----------------------------

Creating the Datasets
---------------------

A empty instance of the :class:`TorsionDriveDataset <qcfractal.interface.collections.TorsionDriveDataset>` 
object (here, named ``My Torsions``) can be constructed as

.. code-block:: python

    >>> client = ptl.FractalClient("localhost:7777")
    >>> ds = ptl.collections.TorsionDriveDataset("My Torsions")


Computational Tasks
-------------------

API
---

.. autoclass:: qcfractal.interface.collections.TorsionDriveDataset
    :members:
    :inherited-members:
