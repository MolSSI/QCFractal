========
QCPortal
========

*QCPortal is a distributed compute and archival platform for quantum chemistry.*


Scaling
-------

QCPortal is aimed at a single user on a laptop up to large multi-PI groups on
dozens of different supercomputers. QCPortal provides a central location to
marshal and distribute data or computation. QCPortal can switch between a
variety of computational queue backends such as:

- `Dask <http://dask.pydata.org/en/latest/docs.html>`_ - A graph-based workflow engine for laptops and small clusters.
- `Fireworks <https://materialsproject.github.io/fireworks/>`_ - A asynchronous Mongo-based distributed queuing system.
- `Parsl <http://parsl-project.org>`_ - High-performance workflows.

Additional backends such as BOINC, Radical Pilot, and Balsam are under active
consideration.

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

**Model Documentation**

Documentation for models and compute records.

* :doc:`record-overview`
* :doc:`results`
* :doc:`procedures`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Records Documentation

   record-overview.rst
   results.rst
   procedures.rst


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
   changelog.rst
