========
QCPortal
========

*QCPortal is a front-end to a QCFractal server which allows the querying,
visualization, manipulation of hosted data.*


QCPortal emphasizes the following virtues:

- **Organize:** Large sets of computations are organized into Collections for easy reference and manipulation.
- **Reproducibility:** All steps of commonly used pipelines are elucidated in the input without additional human intervention.
- **Exploration:** Explorate and query of all data contained within a FractalServer.
- **Visualize:** Plot graphs within Jupyter notebooks or provide 3D graphics of molecules.
- **Accessibility:** Easily share quantum chemistry data with colleagues or the community through accessibility settings.


Collections
-----------

Collections are objects that can reference tens or millions of individual
computations and provide handles to access and visualize this data. There are
many types of collections such as:

- ``ReactionDataset`` - A dataset that allows hold many chemical reactions which can be computed with a large vareity of computational methods.
- ``TorsionDriveDataset`` - A dataset of molecules where torsion scans can be executed with a variety of computational methods.

There are many types of collections and more are being added to index and
organize computations for every use case.

Visualization
-------------

Advanced visualization routines based off Plotly is provided out of the box to
allow interactive statistics and rich visual information. In addition, popular
molecular visualization tools like 3dMol.js provide interactive molecules
within the Jupyter notebook ecosystem.

========

Index
-----


**Getting Started**

* :doc:`install`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   install.rst


**Collections**

Collections are the primary way of viewing and generating new data.

* :doc:`collections`
* :doc:`collection-dataset`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Collections

   collections.rst
   collection-dataset
   collection-torsiondrive

**Records**

Documentation for compute records.

* :doc:`record-overview`
* :doc:`record-result`
* :doc:`record-optimization`
* :doc:`record-api`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Records

   record-overview.rst
   record-result.rst
   record-optimization.rst
   record-api.rst


**Fractal Client**

A Client is the primary user interface to a Fractal server instance.

* :doc:`client`
* :doc:`client-add-query`
* :doc:`client-record-query`
* :doc:`client-new-compute`
* :doc:`client-api`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Fractal Client

   client.rst
   client-add-query.rst
   client-record-query.rst
   client-new-compute.rst
   client-api.rst


**Developer Documentation**

Contains in-depth developer documentation.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Developer Documentation

   glossary.rst
   rest-api.rst
   changelog.rst
