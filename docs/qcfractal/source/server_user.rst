Fractal Server User
====================

The sub-command for the ``qcfractal-server`` CLI which manages user permissions and passwords.

Command Invocation
------------------

.. code-block:: bash

    qcfractal-server user [<options>]


Top-level Options
-----------------

``--base-folder [<folder>]``
    The QCFractal base directory to attach to. Default: ``~/.qca/qcfractal``.

Subcommand Summary
------------------

The ``qcfractal-server user`` CLI allows for manipulation of users through four subcommands:

* ``add``: Add a new user.
* ``show``: Display a user's permissions.
* ``modify``: Change a user's permissions or password.
* ``remove``: Delete a user.

.. _server_user_add:

Add Subcommand
--------------

Command Invocation
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    qcfractal-server user add [<options>] <username>

Command Description
~~~~~~~~~~~~~~~~~~~

This command adds a new user, setting the user's password and permissions. The user must not already exist. 

Arguments
~~~~~~~~~

``<username>``
    The username to add.

``--password [<password>]``
    The password for the user. 
    If this option is not provided, a password will be generated and printed.

``--permissions [<permissions>]``
    Permissions for the user. 
    Allowed values: ``read``, ``write``, ``queue``, ``compute``, ``admin``. 
    Multiple values are allowed. 
    At least one value must be specified. 


.. _server_user_show:

Show Subcommand
---------------

Command Invocation
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    qcfractal-server user show <username>

Command Description
~~~~~~~~~~~~~~~~~~~

This command prints the permissions for a given user.

Arguments
~~~~~~~~~

``<username>``
    The username for which to show permissions.

.. _server_user_modify:

Modify Subcommand
-----------------

Command Invocation
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    qcfractal-server user modify [<options>] <username>

Command Description
~~~~~~~~~~~~~~~~~~~

This command modifys a user's permissions or password.

Arguments
~~~~~~~~~

``<username>``
    The username to modfiy.

``--password [<password>]``
    Change the user's password to a given string. 
    This options excludes ``--reset-password``.

``--reset-password``
    Change the user's password to an auto-generated value. 
    The new password will be printed. 
    This option excludes ``--password``.

``--permissions [<permissions>]``
    Change the user's permissions to the given set. 
    Allowed values: ``read``, ``write``, ``queue``, ``compute``, ``admin``.
    Multiple values are allowed.
    See :ref:`server_user_permissions` for more information.


.. _server_user_remove:

Remove Subcommand
-----------------

Command Invocation
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    qcfractal-server user remove <username>

Command Description
~~~~~~~~~~~~~~~~~~~

This command removes a user.

Arguments
~~~~~~~~~

``<username>``
    The username to remove.

.. _server_user_permissions:

User Permissions
----------------

Five permission types are available:

* ``read`` allows read access to existing records. 
* ``write`` allows write access to existing records and the ability to add new records.
* ``compute`` allows enqueuing new :term:`Tasks <Task>`.
* ``queue`` allows for consumption of compute :term:`Tasks <Task>`.
  This permission is intended for use by a :term:`Manager`.
* ``admin`` allows all permissions.


