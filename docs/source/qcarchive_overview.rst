QCArchive Design
==================

The QCArchive software ecosystem consist of a series of Python modules that
can either be used together or are useful standalone pieces to the
computational molecular sciences community. This ecosystem is constructed to
be used at a single-user, small group level, and multi-PI level while retaining
the ability to scale up to the needs of an entire community of scientist.

In each case it is expected only a small number of users are required to
understand the entire software stack and the primary interaction with the
QCArchive ecoystem will be through the user front-end (QCPortal). Afer the
persistence server instance (QCFractal) is instantiated with a distributed
workflow system and compute the server should largely be able to maintain
itself without user intervention. A diagram of how the ecosystem works in
concert can be seen below:



.. image:: media/boxology_overview.jpg
   :width: 800px
   :alt: QCArchive project structure
   :align: center

1) QCPortal
++++++++++++

- GitHub: |QCPortalBadge|_
- Hardware: Laptop
- Actor: User
- Primary Developer: MolSSI

.. |QCPortalBadge| image:: https://img.shields.io/github/stars/MolSSI/QCPortal.svg?style=social&label=Stars
.. _QCPortalBadge: https://github.com/MolSSI/QCPortal

QCPortal provides a Python-based user front-end experience for users who are interested in exploring data and executing new tasks.

QCPortal uses the REST API of QCFractal to read and write metadata, query complete results, request new computation, and





2) QCFractal
++++++++++++

- GitHub: |QCFractalBadge|_
- Hardware: Persistent Server
- Actor: Power User
- Primary Developer: MolSSI

.. |QCFractalBadge| image:: https://img.shields.io/github/stars/MolSSI/QCFractal.svg?style=social&label=Stars
.. _QCFractalBadge: https://github.com/MolSSI/QCFractal

QCFractal is the primary persistent server that has several main duties:
 - Maintain a database of all completed quantum chemistry results along with metadata that forms higher level collections of results.
 - Maintain a compute queue of all requested and completed tasks. Where each task is a single quantum chemistry result.
 - Submit new tasks to distributed workflow engines and insert complete results into the databse.

3) Distributed Compute
++++++++++++++++++++++

 - Hardware: Persistent Server/Supercomputer
 - Actor: Power User
 - Primary Developer:

The QCArchive project relies on a number of distributed compute workflow
engines to enable a large variety of compute workloads. QCFractal will
interact with each of these projects and at the task level to

These interfaces vary from Python-based API calls to REST API interfaces
depending on the implementation details of the individual tools.

Current distributed compute backends are:
 - `Dask Distributed <http://dask.pydata.org>`_ - Python-based task scheduler.
 - `Fireworks <https://materialsproject.github.io/fireworks/>`_ -

Pending backend implementations include:
 - `RADICAL Cybertools <https://radical-cybertools.github.io>`_ -
 - `BOINC <http://boinc.berkeley.edu>`_ - High throughput volunteer computing.
 - `Balsam <https://balsam.alcf.anl.gov>`_ -

Each of

4) QCEngine
++++++++++++

- GitHub: |QCEngineBadge|_
- Hardware: Local Cluster, Supercomputer, or Cloud Compute
- Actor: Power User

.. |QCEngineBadge| image:: https://img.shields.io/github/stars/MolSSI/QCEngine.svg?style=social&label=Stars
.. _QCEngineBadge: https://github.com/MolSSI/QCEngine

QCEngine a lightweight wrapper around Quantum Chemistry programs so that they consi


5) 3rd Party Services
+++++++++++++++++++++

 - Hardware: Laptop
 - Actor: User/Power User
 - Primary Developer: Computational Molecular Sciences Community


.. _QCSchema: https://github.com/MolSSI/QC_JSON_Schema
