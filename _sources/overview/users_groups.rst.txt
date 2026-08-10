.. _overview_users_groups:

Users, Roles, and Groups
========================

A QCArchive server can require users to log in, and controls what each of them may do. This
page describes how that works from a user's point of view. For the commands to actually
create and modify users, see :ref:`the CLI <server_admin_users>` or
:doc:`the PortalClient <../admin_guide/portal_admin>`.

Not every server enables this. See :ref:`overview_security_disabled` below.


.. _overview_users:

Users
-----

A user is identified by a **username** and authenticates with a **password**. Beyond that,
a user record carries a small amount of descriptive information (full name, organization,
email), an **enabled** flag, exactly one :ref:`role <overview_roles>`, and a list of
:ref:`groups <overview_groups>`.

Usernames may not be empty, contain spaces, or consist only of digits. Passwords must be at
least six characters. Both are rejected by the server rather than silently altered.

Disabling a user is the reversible alternative to deleting one: a disabled user still exists
and still owns whatever they created, but cannot log in. Records and datasets record the
user who created them in their ``creator_user`` field, so deleting a user is not something
to do casually.

.. note::

  A user's own information is available through :meth:`~qcportal.client.PortalClient.get_user`
  with no arguments. Changing your own password does not require an administrative role -
  every role except ``anonymous`` may read and modify its own account.


.. _overview_roles:

Roles
-----

Every user has exactly **one** role, and that role alone determines what the user may do.
Roles are built into the server: the set is fixed, and assigning any other name is an error.

.. table::

  ==============  =============================================================================
   Role            What it permits
  ==============  =============================================================================
   ``admin``       Everything, including managing users and groups
   ``maintain``    Full access to records, datasets, projects, groups, logs and internal jobs,
                   and can change the server information and message of the day. Can read
                   users, but not create or modify them
   ``monitor``     Read-only, and additionally can read access logs, server errors,
                   and internal jobs
   ``submit``      Read, add, modify and delete records, datasets and projects.
                   The usual role for a working user
   ``read``        Read-only access to records, datasets, projects, managers and server
                   information
   ``compute``     Reserved for compute managers claiming and returning tasks
   ``anonymous``   The same read-only access as ``read``, minus the ability to see or change
                   an account. Applied to unauthenticated connections when
                   ``allow_unauthenticated_read`` is enabled - see
                   :ref:`overview_unauthenticated_read`
  ==============  =============================================================================

Two things about roles are worth knowing up front, because they surprise people:

**Roles are not scoped by ownership.** A user with the ``submit`` role may modify or delete
*any* record, dataset, or project on the server, not only the ones they created. There is no
per-object permission; if someone can delete records, they can delete everyone's records.

**Permissions are per resource and action, not per object.** The server asks a single
question - "may this role perform this action on this kind of thing?" - and the answer does
not depend on which particular record or dataset is involved.

If you get an unexpected ``403``, it is because your role does not permit that action at
all. See :ref:`troubleshooting_connection`.


.. _overview_groups:

Groups
------

A server may define **groups**, and users may be members of any number of them. A group has
a name and a description.

Group membership is visible on a user's own information and is included in the session token,
so an application built against the web API can read it and use it for its own purposes.


.. _overview_security_disabled:

Servers with security disabled
------------------------------

Authentication is optional. A server started with ``enable_security: false`` does not
authenticate anyone, and every user-facing operation is permitted without logging in -
reading and submitting records, creating datasets and projects, viewing managers and logs.

This is the default for a :doc:`snowflake <snowflake>`, which is why nothing in the
quickstart asks you for a password.

The exception is user and group management itself. Those endpoints require security to be
enabled, and refuse to run otherwise:

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> client.list_users()
      qcportal.client_base.PortalRequestError: Request failed: Cannot access 'users' with security disabled

This covers listing, adding, modifying and deleting users and groups, and the "my own
account" endpoints. Everything else behaves normally.


.. _overview_unauthenticated_read:

Read-only access without logging in
-----------------------------------

A server with security *enabled* can still allow anonymous browsing, controlled by
``allow_unauthenticated_read``:

.. table::

  ==================================  ==========================================================
   ``allow_unauthenticated_read``      Connecting without credentials
  ==================================  ==========================================================
   ``true`` (the default)              Permitted, with the ``anonymous`` role - read-only access
                                       to records, datasets, projects, managers, and server
                                       information
   ``false``                           Refused, with ``Server requires login``
  ==================================  ==========================================================

The ``anonymous`` role is read-only. Submitting anything, or modifying anything, requires
logging in as a user whose role permits it.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      >>> # No credentials - works if the server allows unauthenticated read
      >>> client = PortalClient("https://ml.qcarchive.molssi.org")

      >>> # With credentials
      >>> client = PortalClient("https://ml.qcarchive.molssi.org",
      ...                       username="ben", password="<password>")

See :doc:`../user_guide/client_setup` for keeping credentials out of your scripts, and
:ref:`server_configuration` for the two settings above.


Managing users and groups
-------------------------

Creating and modifying users requires the ``admin`` role, and can be done either way:

* :ref:`From the command line <server_admin_users>`, with ``qcfractal-server user``. This is
  the usual route, and the only one available before any user exists - it runs against the
  database directly rather than through the API.
* :doc:`From a PortalClient <../admin_guide/portal_admin>`, which is more convenient for
  scripting against a running server.

**Groups can only be managed through a client.** There is no ``qcfractal-server group``
subcommand; use :meth:`~qcportal.client.PortalClient.add_group` and friends.


Users and Groups API Reference
------------------------------

* :class:`qcportal.auth.models.UserInfo`
* :class:`qcportal.auth.models.GroupInfo`

* PortalClient methods

  * :meth:`~qcportal.client.PortalClient.get_user`, :meth:`~qcportal.client.PortalClient.list_users`
  * :meth:`~qcportal.client.PortalClient.add_user`, :meth:`~qcportal.client.PortalClient.modify_user`,
    :meth:`~qcportal.client.PortalClient.delete_user`
  * :meth:`~qcportal.client.PortalClient.change_user_password`
  * :meth:`~qcportal.client.PortalClient.list_groups`, :meth:`~qcportal.client.PortalClient.get_group`,
    :meth:`~qcportal.client.PortalClient.add_group`, :meth:`~qcportal.client.PortalClient.delete_group`
