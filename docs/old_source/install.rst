Install QCFractal
=================

You can install QCFractal with ``conda`` (recommended) or with ``pip`` (with some caveats).

The below commands install QCFractal and its required dependencies, but *not* any of the quantum
chemistry codes nor the software to run :term:`Queue Managers <Manager>`. This is done to avoid requiring *all* software
which *can* interface with QCFractal, and instead requires the user to obtain the software they individually *require*.

.. _conda-install:

Conda
-----

You can install QCFractal using `conda <https://www.anaconda.com/download/>`_:

.. code-block:: console

    >>> conda install qcfractal -c conda-forge

This installs QCFractal and its dependencies. The QCFractal package is maintained on the
`conda-forge channel <https://conda-forge.github.io/>`_.

Conda Pre-Created Environments
++++++++++++++++++++++++++++++

QCFractal can also be installed through pre-configured environments you can pull through our Conda Channel:

.. code-block:: console

   >>> conda env create qcarchive/{environment name}
   >>> conda activate {environment name}

The environments are created from the YAML files hosted on the Anaconda Cloud, which then need to be activated
to use. You can find all of the environments `here <https://anaconda.org/QCArchive/environments>`_.

If you want to use a different name than the environment file, you can add a ``-n {custom name}`` flag to the
``conda env`` command.

The environments must be installed as new environments and cannot be installed into existing ones.

The environments are designed to provide pre-built environments which include additional programs beyond QCFractal
itself which are designed for use in production or practical experimentation. For instance, the ``qcf-manager-openff``
environment also installs a couple quantum chemistry programs, a distributed compute :term:`Queue Adapter`, and a
service which QCFractal can run. This environment can be deployed for immediate use on a remote compute
site (e.g. a cluster) and connect to a QCFractal instance to consume compute tasks.

Pip
---

.. warning::

   Installing QCFractal from PyPi/``pip`` requires an existing PostgreSQL installation on your computer. Whether that be
   through a native install on your device (e.g. managed clusters), a direct installer, ``yum`` install, a ``conda``
   install, or otherwise; it must be installed first or the ``Psycopg2`` package will complain about missing the
   ``pg_config``. Installation of PostgreSQL manually is beyond the scope of these instructions, so we recommend
   either using a :ref:`Conda install of QCFractal <conda-install>` or contacting your systems administrator.

If you have PosgreSQL installed already, you can also install QCFractal using ``pip``:

.. code-block:: console

   >>> pip install qcfractal


Test the Installation
---------------------

.. note::

   There are several optional packages QCFractal can interface with for additional features such as visualization,
   :term:`Queue Adapters <Queue Adapter>`, and services. These are not installed by default and so you can expect many
   of the tests will be marked with ``skip`` or ``s``.

You can test to make sure that Fractal is installed correctly by first installing ``pytest``.

From ``conda``:

.. code-block:: console

   >>> conda install pytest -c conda-forge

From ``pip``:

.. code-block:: console

   >>> pip install pytest

Then, run the following command:

.. code-block::

   >>> pytest --pyargs qcfractal

QCFractal ships with a small testing plugin which should be automatically detected and gives you access to the
``--runslow`` and ``--runexamples`` PyTest CLI flags. The ``--runslow`` flag tells the testing suite to run any test
the developers think are a bit more time consuming than the others. Without this flag, you will see many tests (such
as those for the CLI) skipped.


Developing from Source
----------------------

If you are a developer and want to make contributions QCFractal, you can access the source code from
`github <https://github.com/molssi/qcfractal>`_.
