QCPortal Installation & Setup
===============================

Installation through conda
--------------------------

The QCPortal package can be installed via `conda/anaconda <https://www.anaconda.com>`_
or `mamba <https://github.com/mamba-org/mamba>`_. The packages are available from the
`conda-forge <https://anaconda.org/conda-forge/qcportal>`_ channel.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

        conda create -n qcportal qcportal -c conda-forge
        conda activate qcportal


.. _qcportal_setup_configfile:

Configuration File
------------------

Typically, you pass in the username and password to the :class:`~qcportal.client.PortalClient` constructor.
However, for frequently-used servers, or for added security, the username and passwords
used to connect to remote QCFractal servers can be placed in a configuration file. This file is then
read by the :meth:`PortalClient.from_file <qcportal.client.PortalClient.from_file>` method.

This file can be placed anywhere, with the path passed into that function, or can be placed in the current
working directory or the ``~/.qca`` directory; on the latter two cases it should be given the name
``qcportal_config.yaml``. See :ref:`qcportal_connecting_file`.

Single Server
~~~~~~~~~~~~~

If you are only interested in a single server, then the configuration file can just
contain the address and user information.

.. tab-set::

  .. tab-item:: CONFIG FILE

    .. code-block:: yaml

      address: https://qcademo.molssi.org
      username: your_username
      password: Secret_Password

Multiple Servers
~~~~~~~~~~~~~~~~

If you are working with multiple servers, then the configuration file contains sections with a name, and then
the address and other options. The name is arbitrary and is for the user to
differentiate between different servers.

.. tab-set::

  .. tab-item:: CONFIG FILE
    
    .. code-block:: yaml

        qca_demo_server:
          address: https://qcademo.molssi.org
          username: your_username
          password: Secret_Password

        group_server:
          address: http://192.168.123.123:7777
          username: your_username
          password: Secret_Password

The path to this file and the section name can passed to the
:meth:`PortalClient.from_file <qcportal.client.PortalClient.from_file>` method.


.. _qcportal_setup_envvar:

Environment Variables
---------------------

The information needed for constructing a client can also be read from environment variables.
See :meth:`PortalClient.from_env <qcportal.client.PortalClient.from_env>`

.. tab-set::

  .. tab-item:: CONFIG FILE

    .. code-block:: bash

      export QCPORTAL_ADDRESS="https://qcademo.molssi.org"
      export QCPORTAL_USERNAME="your_username"
      export QCPORTAL_PASSWORD="Secret_Password"
      export QCPORTAL_CACHE_DIR="/path_to_cache"


