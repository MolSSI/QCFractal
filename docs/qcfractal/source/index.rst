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

   install.rst
   community.rst
   setup_compute.rst
   setup_server.rst
   roadmap.rst

**Records Documentation**

The records created from adding additional compute.

* :doc:`results`
* :doc:`procedures`
* :doc:`services`
* :doc:`flow`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Records Documentation

   results.rst
   procedures.rst
   services.rst
   flow.rst

**Manager Documentation**

Setting up and running Fractal's Queue Managers on your system.

* :doc:`managers`
* :doc:`managers_config_api`
* :doc:`managers_samples`
* :doc:`managers_faq`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Manager Documentation

   managers.rst
   managers_config_api.rst
   managers_samples.rst
   managers_faq.rst
   managers_detailed.rst

**Developer Documentation**

Contains in-depth developer documentation.

* :doc:`qcarchive_overview`
* :doc:`glossary`
* :doc:`changelog`
* :doc:`glossary`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Developer Documentation

   qcarchive_overview.rst
   api.rst
   database_design.rst
   glossary.rst
   dev_guidelines.rst
   changelog.rst

