Queue Manager Frequent Questions and Issues
===========================================

This page documents some of the frequent questions and issues we see with the
Queue Managers. If this page and none of the other documentation pages have
answered your question, please ask `on GitHub <https://github.com/MolSSI/QCFractal/>`_ or
`join our Slack group <https://join.slack.com/t/qcdb/shared_invite/enQtNDIzNTQ2OTExODk0LWM3OTgxN2ExYTlkMTlkZjA0OTExZDlmNGRlY2M4NWJlNDlkZGQyYWUxOTJmMzc3M2VlYzZjMjgxMDRkYzFmOTE>`_
to get assistance.

Common Questions
----------------

How do I get more information from the Manger?
++++++++++++++++++++++++++++++++++++++++++++++

Turn on ``verbose`` mode, either add the ``-v`` flag to the CLI, or set the
``common.verbose`` to ``True`` in the YAML file. Either will make your logger much more loud.

Can I start more than one Manager at a time?
++++++++++++++++++++++++++++++++++++++++++++

Yes. Check with your Sys. Admins though to find out their policy on multiple
processes running on the Head Nodes

Can I connect to a Fractal Server besides MolSSI's?
+++++++++++++++++++++++++++++++++++++++++++++++++++

Yes! Just change the ``server.fractal_uri`` argument.

Can I connect to a more tan one Fractal Server
++++++++++++++++++++++++++++++++++++++++++++++

Yes and No. Each :term:`Manager` can only connect to 1 :term:`Fractal Server<Server>`, but
you can start multiple managers with different config files pointing to different
:term:`Fractal Servers<Server>`

How do I get a username and password to help add my compute?
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

`Join our Slack group! <https://join.slack.com/t/qcdb/shared_invite/enQtNDIzNTQ2OTExODk0LWM3OTgxN2ExYTlkMTlkZjA0OTExZDlmNGRlY2M4NWJlNDlkZGQyYWUxOTJmMzc3M2VlYzZjMjgxMDRkYzFmOTE>`_
We would love to talk to you and help get you contributing as well!

I have this issue, here is my config file...
++++++++++++++++++++++++++++++++++++++++++++

Great! I only ask that you please **remove the password from the config file
before posting it.** If we see a password, we'll do our best to delete it, but
that does not ensure someone did not see it.


Common Issues
-------------

This documents some of the common issues we see.

Jobs are quickly started and die without error
++++++++++++++++++++++++++++++++++++++++++++++

We see this problem with Dask often and the most common case is the Head Node (landing node, login node, etc.)
has an ethernet adapter with a different name than the compute nodes. You can check this by running the command
``ip addr`` on both the head node and a compute node (either through an interactive job or a job which simply writes
the output of that command to a file).

You'll see alot of output which looks like this:

.. code-block::

    1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN qlen 1
        link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
        inet 127.0.0.1/8 scope host lo
    3: eno49.4010@eno49: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP qlen 1000
        inet 10.XX.Y.Z/16 brd 10.XX.255.255 scope global eno49.4010
    4: eno49.4049@eno49: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP qlen 1000
        inet 198.XX.YYY.ZZZ/24 brd 198.XX.252.255 scope global eno49.4049

and a bunch of other lines in between I have removed, the lines I have left with the ``XX``, ``YYY`` and ``ZZZ`` will
have actual numbers there.
There are a few important things: the headers (``lo``, ``eno.49...``, yours will be different) and the addresses where
the ``XX`` placeholders are. First thing: Ignore the ``lo`` adapter, every machine should have one. On the head node,
you will probably see an adapter who has a ``inet`` that looks like a normal IP address, and probably one which
looks like it has a ``10.something`` IP address. The compute node will probably have an adapter which is
only the ``10.something``. These ``10.something`` IP addresses are often intranet communication only, so the compute
nodes cannot reach the broader internet. What will be different is name of the ethernet adapter housing
the ``10.something`` will be *different* on the head node and the compute node.

In this case, in your YAML file, add a line in ``dask`` called ``interface`` and set it to the name of the
adapter which is shared. So for this it would be:

.. code-block:: yaml

    dask:
     interface: "eno49.4049"

plus all the rest of your YAML file. You can safely ignore the bit after the ``@`` sign.

If there isn't a shared adapter name, try this instead:

.. code-block:: yaml

    dask:
     ip: "10.XX.Y.Z"

And replace the ``.XX.Y.Z`` bit with the code which has the intranet IP of the *head node*. This option
acts as a pass through to the Dask :term:`Worker` call and tells the worker to try and connect to the
head node at that IP address. If that still doesn't work, contact us.


Other variants:

- "My jobs start and stop instantly"
- "My jobs restart forever"


My Conda Environments are not Activating
++++++++++++++++++++++++++++++++++++++++

You likely have to ``source`` the Conda ``profile.d`` again first. See also
`<https://github.com/conda/conda/issues/8072>`_


My jobs appear to be running, but only one (or few) workers are starting
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

If the jobs appear to be running (and the Manager is reporting they return successfully),
a few things may be happening.

- If jobs are completing very fast, the :term:`Adapter` may not feel like it needs to start more
  :term:`workers<Worker>`, which is fine.
- Check your ``manger.max_tasks`` arg to pull more :term:`tasks<Task>` from the :term:`Server` to fill
  the jobs you have started.