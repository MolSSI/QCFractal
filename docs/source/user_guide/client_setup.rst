QCPortal Installation & Setup
===============================

Installation through conda
--------------------------

The QCPortal package can be installed via `conda/anaconda <https://www.anaconda.com>`_
or `mamba <https://github.com/mamba-org/mamba>`_. The packages exist under the
`QCArchive organization <https://anaconda.org/QCArchive>`_ on Anaconda.

.. tab-set::

  .. tab-item:: SHELL

    .. code-block:: bash

        conda create -n qcportal qcportal -c conda-forge
        conda activate qcportal


.. _qcportal_setup_configfile:

Configuration File
------------------

The username and password used to connect to the remote QCFractal server
can be placed in a configuration file. This file is then
read by the :meth:`~PortalClient.from_file <qcportal.client.PortalClient.from_file>` function.

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
:meth:`~qcportal.client.PortalClient.from_file` function.
