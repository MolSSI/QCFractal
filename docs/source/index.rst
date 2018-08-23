.. QCFractal documentation master file, created by
   sphinx-quickstart on Fri Aug 17 09:45:43 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=========
QCFractal
=========

*QCFractal is a distributed compute and database platform for quantum chemistry.*

Workflows
---------

QCFractal supports a number of workflow types such:
 - Ensembles of single point quantum chemistry comptuations.
 - Procedures such as geometry optimization, finite different gradients and Hessians, and complete basis set extrapolations.
 - Complex workflows such as the `OpenFF <http://openforcefield.org>`_ torsion scan workflow as shown below:

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
 - `Fireworks <https://materialsproject.github.io/fireworks/>`_ - A asynchronus Mongo-based distributed queuing system.

Additional backends such as BOINC, Radical Pilot, and Balsam are under active
consideration.

QCArchive
---------

This module is part of the QCArchive project wich sets out to answer the
fundamental question of "How do we compile, aggregate, query, and share quantum
chemistry data to accelerate the understanding of new method performance,
fitting of novel force fields, and supporting the incredible data needs of
machine learning for computational molecular science?"

The QCArchive project is made up of three primary modules:
 - `QCSchema <https://github.com/MolSSI/QC_JSON_Schema>`_ - A key/value schema for quantum chemistry.
 - `QCEngine <https://github.com/MolSSI/QCEngine>`_ - A computational middleware to provide IO to a variety of quantum chemistry programs.
 - `QCFractal <https://github.com/MolSSI/QCFractal>`_ - A distributed compute and database platform powered by QCEngine and QCSchema.

The QCArchive project's primary supports comes from `The Molecular Sciences Software Institute <https://molssi.org>`_.

========

Table of Contents
=================

.. toctree::
   :maxdepth: 2
   :caption: Getting Started

   install
   community
   roadmap

.. toctree::
   :maxdepth: 1
   :caption: Developer Documentation

   changelog
   dev_guidelines
