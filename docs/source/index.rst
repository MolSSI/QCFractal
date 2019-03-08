.. QCFractal documentation master file, created by
   sphinx-quickstart on Fri Aug 17 09:45:43 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=========
QCFractal
=========

*QCFractal is a distributed compute and archival platform for quantum chemistry.*

Workflows
---------
QCFractal supports several high-throughput contexts:
 - Ensembles of single point quantum chemistry computations.
 - Procedures such as geometry optimization, finite different gradients and Hessians, and complete basis set extrapolations.
 - Complex scenarios such as the `OpenFF <http://openforcefield.org>`_ torsion scan workflow:

.. image:: media/openff_torsion_workflow.jpg
   :width: 800px
   :alt: OpenFF Torsion Workflow example
   :align: center


Scaling
-------

QCFractal is aimed at a single user on a laptop up to large multi-PI groups on
dozens of different supercomputers. QCFractal provides a central location to
marshal and distribute data or computation. QCFractal can switch between a
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
* :doc:`setup_compute`
* :doc:`setup_server`
* :doc:`community`
* :doc:`roadmap`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   install
   community
   setup_compute
   setup_server
   roadmap

**Portal Documentation**

Portal is the primary user interface to a Fractal server instance.

* :doc:`portal-client`
* :doc:`portal-collections`
* :doc:`portal-molecule`

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Portal Documentation

   portal-client
   portal-collections
   portal-molecule

**Fractal Documentation**

Fractal contains the sever instance, database, and distributed compute queue.

* :doc:`fractal-results`
* :doc:`fractal-procedures`
* :doc:`fractal-services`
* :doc:`fractal-flow`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Fractal Documentation

   fractal-results
   fractal-procedures
   fractal-services
   fractal-flow

**Developer Documentation**

Contains in-depth developer documentation.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Developer Documentation

   qcarchive_overview
   api
   database_design
   glossary
   dev_guidelines
   changelog

