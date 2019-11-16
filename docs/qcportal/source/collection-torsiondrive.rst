TorsionDrive Dataset
====================

See also the `QCArchive example <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/torsiondrive_datasets.html>`_ for TorsionDrive datasets.

TorsionDriveDatasets are sets of TorsionDrive computations where the primary
index a set of starting molecules and each column is represented by a new
TorsionDrive specification. This Dataset is a procedure-style dataset where a
record of the :term:`ObjectId` of each TorsionDrive computation are recorded
in the metadata.

Querying
--------

Visualizing
-----------

Creating
--------

A empty TorsionDriveDataset can be constructed by choosing a dataset name.

.. code-block:: python

    client = ptl.FractalClient("localhost:7777")
    ds = ptl.collections.TorsionDriveDataset("My Torsions")


Computing
---------

API
---

.. autoclass:: qcfractal.interface.collections.TorsionDriveDataset
    :members:
    :inherited-members:
