Administration & maintenance
=====================================

.. _server_admin_startstop:

Starting and Stopping
---------------------


.. _server_admin_backup:

Backup and Restore
---------------------


.. _server_admin_upgrade:

Upgrading
---------------------


.. _server_admin_security:

Security
---------------------


.. _server_admin_users:

Users and Passwords
---------------------

The ``qcfractal-server user`` command allows management of users for the server.
To list the current users, use the ``qcfractal-server user list`` subcommand.
To add a new user, use the ``qcfractal-server user add`` command. To add a user,
a role needs to be set for them. The following roles are accepted by QCFractal:

+-----------+-----------------------------------------------------------------+
| Role      | Description                                                     |
+===========+=================================================================+
| admin     | Administer and manage the server.                               |
+-----------+-----------------------------------------------------------------+
| maintain  | Similar to admin, but with some restrictions.                   |
+-----------+-----------------------------------------------------------------+
| monitor   | User is allowed to read information, but not submit jobs.       |
+-----------+-----------------------------------------------------------------+
| submit    | User is allowed to read information and submit jobs.            |
+-----------+-----------------------------------------------------------------+
| read      | User is allowed to read job information, but not server         |
|           | information.                                                    |
+-----------+-----------------------------------------------------------------+
| anonymous | Same as read, but does not have access to user information.     |
+-----------+-----------------------------------------------------------------+
| compute   | User can pull jobs off the manager to run them.                 |
+-----------+-----------------------------------------------------------------+


.. _server_admin_accesslog:

Access Logs and Geoip
---------------------
