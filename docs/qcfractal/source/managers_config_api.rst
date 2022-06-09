Queue Manager API
=================

This page documents **all** valid options for the YAML file inputs to the config manager. This first section outlines
each of the headers (top level objects) and a description for each one. The final file will look like the following:

.. code-block:: yaml

    common:
        option_1: value_for1
        another_opt: 42
    server:
        option_for_server: "some string"


This is the complete set of options, auto-generated from the parser itself, so it should be accurate for the given
release. If you are using a developmental build or want to see the schema yourself, you can run the
``qcfractal-manager --schema`` command and it will display the whole schema for the YAML input.

Each section below here is summarized the same way, showing all the options for that YAML header in the form of their
`pydantic <https://pydantic-docs.helpmanual.io/>`_ API which the YAML is fed into in a one-to-one match of options.


.. autopydantic_model:: qcfractal.cli.qcfractal_manager.ManagerSettings


common
------

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.CommonManagerSettings


.. _managers_server:

server
------

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.FractalServerSettings


manager
-------

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.QueueManagerSettings


cluster
-------

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.ClusterSettings


dask
----

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.DaskQueueSettings


parsl
-----

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.ParslQueueSettings


executor
++++++++

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.ParslExecutorSettings


provider
++++++++

.. autopydantic_model:: qcfractal.cli.qcfractal_manager.ParslProviderSettings
