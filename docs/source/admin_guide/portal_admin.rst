Administering a QCFractal server with PortalClient
==================================================

This page shows how to administer a running QCFractal server programmatically
using the Python client API provided by qcportal's PortalClient.

The examples below assume you can connect to the server with an administrative
account (or a user granted the appropriate permissions for the action).

.. note::

   These administrative APIs correspond to the same capabilities exposed by the
   command-line interface described in :doc:`cli_admin` but are accessible from
   Python for scripting and automation.


Getting started: connecting and authentication
----------------------------------------------

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from qcportal import PortalClient

      # Connect with an admin-capable account
      client = PortalClient(
          "http://localhost:7777",
          username="admin",
          password="your-admin-password",
      )


Users: add, list, enable/disable, change passwords, delete
----------------------------------------------------------

User objects are represented by :class:`~qcportal.auth.models.UserInfo`.

For what users, roles, and groups *are* - the list of roles and what each permits, what
happens on a server with security disabled, and how unauthenticated access works - see
:doc:`../overview/users_groups`. This section covers the administrative operations.

.. note::

  Roles are not scoped by ownership. A user with the ``submit`` role may modify or delete
  any record or dataset on the server, not only the ones they created.

List users and view details
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from qcportal.auth import UserInfo

      # List all users
      users = client.list_users()
      for u in users:
          print(u.username, u.role, u.enabled, u.groups)

      # Get a specific user (by username or id)
      alice = client.get_user("alice")
      print(alice)

Add a new user
^^^^^^^^^^^^^^

When creating a user you provide a UserInfo (without id) and optionally an initial password.
If you omit the password, the server will generate one and return it.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from qcportal.auth import UserInfo

      # Minimal required fields: username, role, enabled
      new_user = UserInfo(
          username="alice",
          role="submit",         # see the list of roles above
          groups=["chemistry"],  # optional group memberships
          enabled=True,
          fullname="Alice Example",
          email="alice@example.org",
      )

      # Let the server generate a password
      generated_pw = client.add_user(new_user)
      print("Generated password:", generated_pw)

      # Or specify an initial password yourself
      client.add_user(new_user, password="ChangeMe123")

Enable/disable a user or update fields
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use modify_user with a UserInfo that includes the user's id (you can obtain it via get_user).

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      # Disable a user account
      bob = client.get_user("bob")
      bob.enabled = False
      bob = client.modify_user(bob)
      print("Bob enabled?", bob.enabled)

      # Change role or groups
      bob.role = "maintain"
      bob.groups = sorted(set(bob.groups + ["theory"]))
      bob = client.modify_user(bob)

Change a user's password
^^^^^^^^^^^^^^^^^^^^^^^^

Admins can change any user's password. Users can also change their own
password by omitting the username.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      # Admin changes a user's password (returns the new password)
      new_pw = client.change_user_password("alice", new_password="BetterPW!1")

      # Ask the server to generate a random password
      random_pw = client.change_user_password("alice")

      # User changes their own password (assumes logged in as alice)
      # client = PortalClient(..., username="alice", password="old")
      # new_pw = client.change_user_password(new_password="my-new-secret")

Delete a user
^^^^^^^^^^^^^

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      client.delete_user("alice")


Groups: create, list, get, delete
---------------------------------

Groups are described by :class:`~qcportal.auth.models.GroupInfo` and can be used
for organization and authorization policies.

List and get groups
^^^^^^^^^^^^^^^^^^^

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from qcportal.auth import GroupInfo

      groups = client.list_groups()
      for g in groups:
          print(g.groupname, g.description)

      chem = client.get_group("chemistry")
      print(chem)

Create and delete groups
^^^^^^^^^^^^^^^^^^^^^^^^

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from qcportal.auth import GroupInfo

      new_group = GroupInfo(groupname="chemistry", description="Chemistry users")
      client.add_group(new_group)

      # Remove a group (users will have it removed from their memberships)
      client.delete_group("chemistry")

.. note::

   Adding/removing users to groups is done by updating the user's ``groups`` field
   via :meth:`~qcportal.client.PortalClient.modify_user`.


Compute managers: viewing manager information
---------------------------------------------

You can query compute manager registrations and see their status and capabilities.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      # List all managers (iterator yields ComputeManager models)
      it = client.query_managers(limit=100)
      managers = list(it)
      for m in managers:
          print(m.name, m.status, m.cluster, m.hostname, m.compute_tags)

      # Filter by name or status. Managers are either 'active' or 'inactive'
      for m in client.query_managers(name=["my_manager"], status=["active"]):
          print(m)

      # Ask which managers could handle a task with certain tags/programs
      possible = client.query_active_managers(
          compute_tag=["cpu-ephemeral", "bigmem"],
          programs={"psi4": ["1.8"], "torchani": ["2.2"]},
      )
      print("Candidate managers:", possible)


Access log: view and clear
--------------------------

The server access log tracks who accessed which API, and when.

Query access logs
^^^^^^^^^^^^^^^^^

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from datetime import timedelta

      from qcportal.utils import now_at_utc

      # Recent entries for a specific user
      one_day_ago = now_at_utc() - timedelta(days=1)
      for entry in client.query_access_log(user=["alice"], after=one_day_ago, limit=100):
          print(entry.timestamp, entry.user, entry.module, entry.method, entry.full_uri)

      # Summarize accesses per day
      summary = client.query_access_summary(group_by="day", after=one_day_ago)
      for day, entries in summary.entries.items():
          print(day, sum(x.count for x in entries))

Delete old access logs
^^^^^^^^^^^^^^^^^^^^^^

.. danger::

   Deleting log entries is permanent. Consider exporting before deletion.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from datetime import timedelta

      from qcportal.utils import now_at_utc

      cutoff = now_at_utc() - timedelta(days=30)
      deleted = client.delete_access_log(before=cutoff)
      print("Deleted entries:", deleted)


Internal jobs: view, cancel, delete
-----------------------------------

Internal jobs are server-maintained background jobs. You can query their status,
request cancellation, or delete completed/error entries.

.. tab-set::

  .. tab-item:: PYTHON

    .. code-block:: py3

      from qcportal.internal_jobs import InternalJobStatusEnum

      # Query internal jobs (iterator)
      jobs = list(client.query_internal_jobs(status=[InternalJobStatusEnum.running], limit=50))
      for j in jobs:
          print(j.id, j.name, j.status, j.modified_on)

      # Get a specific job
      job = client.get_internal_job(job_id=123)
      print(job)

      # Cancel a job (asks server to stop it)
      client.cancel_internal_job(job_id=123)

      # Delete a finished/failed job entry
      client.delete_internal_job(job_id=123)


Tips and notes
--------------

- Permissions: Some actions require elevated privileges. Ensure the account you
  authenticate with has the necessary role/policies on the server.
- Validation: Helper functions like :func:`~qcportal.auth.models.is_valid_username`,
  :func:`~qcportal.auth.models.is_valid_password`, and
  :func:`~qcportal.auth.models.is_valid_groupname` validate inputs and are invoked
  by client methods. Errors will be raised if inputs are invalid.
- Pagination: Many query methods return iterators (eg, managers, access logs,
  internal jobs). Iterate or cast to ``list`` to pull results.
