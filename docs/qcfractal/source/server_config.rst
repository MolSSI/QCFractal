Fractal Server Config
=====================

This page documents the valid options for the YAML file inputs to the :doc:`Config File <server_config>`.
This first section outlines each of the headers (top level objects) and a description for each one.
The final file will look like the following:

.. code-block:: yaml

    common:
        option_1: value_for1
        another_opt: 42
    server:
        option_for_server: "some string"

Command Invocation
------------------

.. code-block:: bash

    qcfractal-server config [<options>]

Command Description
-------------------

Show the current config file at an optional location.

Looks in the default location if no arg is provided

Options
-------

``--base-folder [<folder>]``
    The QCFractal base directory to attach to. Default: ``~/.qca/qcfractal``

Config File Complete Options
----------------------------

The valid top-level YAML headers are the parameters of the ``FractalConfig`` class.

.. autoclass:: qcfractal.config.FractalConfig
   :members:

``database``
************

.. autoclass:: qcfractal.config.DatabaseSettings
   :members:

``fractal``
***********

.. autoclass:: qcfractal.config.FractalServerSettings
   :members:

``view``
********

.. autoclass:: qcfractal.config.ViewSettings
   :members:
