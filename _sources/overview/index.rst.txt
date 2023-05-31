QCArchive Overview
=====================================


.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Contents:

   glossary
   tasks_services
   internal_jobs
   snowflake

Introduction
============

QCArchive is a platform that makes running large numbers of quantum chemistry calculations in a
robust and scalable manner accessible to computational chemists.
QCArchive is designed to handle thousands to millions of computations,
storing them in a database for later sharing, retrieval and analysis, or export.


Motivation
-------------

There is a growing need for enormous amounts of high-accuracy data for a variety of purposes, including
AI/ML and method development. However, while running a few computations is relatively straightforward,
as the number of computations required increases, the complexity of managing and running them increases
dramatically.

This complexity increase is due to a variety of factors, but is primarily driven by the bespoke nature
of running different QM codes, the coordination of distributed computation resources,
as well as handling of errors that inevitably appear as the number of computations scale up.


While there are a variety of distributed computation platforms available, they
tend to be very general and require a large amount of customization by chemists to run quantum chemistry
calculations, whereas QCArchive itself is build around the concepts that computational chemists will find
familiar. In reality, QCArchive utilizes some of these tools (such as `Parsl <https://parsl-project.org>`_)
to achieve its goals.

The overarching goal of the QCArchive project is to make creating and managing these kinds of datasets
as easy as possible for chemists who know some Python, but are unfamiliar with distributed computing at this scale.


Overall Architecture
--------------------

.. image:: ../graphics/architecture_simple.svg
  :align: center

QCArchive contains three main components. Central to the architecture is server software (QCFractal) that
is responsible for storing and managing data. The server is accessed by a client (QCPortal) that provides
a Python-native way to submit and manage calculations.

A third component is the workers (QCFractalCompute). This is responsible for running the calculations.

In a typical setup, the server is run on a dedicated machine, while the client is run on a user's machine.
The workers are set up on a supercomputing cluster, with a process on the head node responsible for
coordinating job submission and monitoring (although other setups are possible).

.. note::

  QCPortal is not the only way to access the server. The server exposes a JSON-based web API, and so
  any software written in any language that can make requests to these kinds of APIs can be used.

If you are just interested in connecting to a server and running computations, you only need the client on your
local computer. For that information, see the :doc:`../user_guide/index`.

For information about setting up a server and/or workers see :doc:`../admin_guide/index`.

Source code
-----------

The source code is available on github at `<https://github.com/MolSSI/QCFractal.git>`_. Packages are also
made available from PyPI and conda-forge (see :doc:`../user_guide/client_setup` and :doc:`../admin_guide/setup`).

