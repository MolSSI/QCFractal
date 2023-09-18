QCArchive Quickstart
=====================================

QCArchive is a data generation and retrieval platform specialized for quantum chemistry calculation.
These quickstart tutorials demonstrate the basics of retrieving results
and submitting computations using the QCArchive Python API.

Depending on your use case for QCArchive, there are many things you might try to do.
For example, you might retrieve specific computations (records) by ID, query the database for particular computations,
submit a computation, or submit and  work with a dataset.

These Quickstart tutorials are intended to cover the basics of all of these use cases.

Installation
------------

QCArchive can be installed using the `conda` package manager.
It is recommended to install QCArchive in its own environment.

.. code-block:: bash

   conda create -n qcportal -c conda-forge qcportal
   conda activate qcportal

Overview Tutorials
------------------
We recommend starting with our 15 minutes to QCArchive tutorial.

.. toctree::
   :maxdepth: 1
   :caption: Overview:
   
   qca_15min.ipynb


Tutorials
---------

.. toctree::
   :maxdepth: 1
   :caption: Quickstart Guides:

   record_retrieval.ipynb
   record_query.ipynb
   