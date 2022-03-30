Queue Manager Frequent Questions and Issues
===========================================

This page documents some of the frequent questions and issues we see with the
Queue Managers. If this page and none of the other documentation pages have
answered your question, please ask `on GitHub <https://github.com/MolSSI/QCFractal/>`_ or
|join_slack_lower|_ to get assistance.

Common Questions
----------------

How do I get more information from the Manger?
++++++++++++++++++++++++++++++++++++++++++++++

Turn on ``verbose`` mode, either add the ``-v`` flag to the CLI, or set the
``common.verbose`` to ``True`` in the YAML file. Setting this flag will produce
much more detailed information. This sets the loggers to ``DEBUG`` level.

In the future, we may allow for different levels of increased verbosity, but for now there is
only the one level.

Can I start more than one Manager at a time?
++++++++++++++++++++++++++++++++++++++++++++

Yes. This is often done if you would like to create multiple :term:`task<Task>` :term:`tags<Tag>` that
have different resource requirements or spin up managers that can access
different resources. Check with your cluster administrators though to find out
their policy on multiple processes running on the clusters head node.

You can reuse the same config file, just invoke the CLI again.

Can I connect to a Fractal Server besides MolSSI's?
+++++++++++++++++++++++++++++++++++++++++++++++++++

Yes! Just change the ``server.fractal_uri`` argument.

Can I connect to more than one Fractal Server
+++++++++++++++++++++++++++++++++++++++++++++

Yes and No. Each :term:`Manager` can only connect to a single :term:`Fractal Server<Server>`, but
you can start multiple managers with different config files pointing to different
:term:`Fractal Servers<Server>`.

How do I help contribute compute time to the MolSSI database?
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

|join_slack_upper|_!
We would love to talk to you and help get you contributing as well!

I have this issue, here is my config file...
++++++++++++++++++++++++++++++++++++++++++++

Happy to look at it! We only ask that you please **remove the password from the config file before posting it.**
If we see a password, we'll do our best to delete it, but
that does not ensure someone did not see it.


Common Issues
-------------

This documents some of the common issues we see.

Jobs are quickly started and die without error
++++++++++++++++++++++++++++++++++++++++++++++

We see this problem with Dask often and the most common case is the head node (landing node, login node, etc.)
has an ethernet adapter with a different name than the compute nodes. You can check this by running the command
``ip addr`` on both the head node and a compute node (either through an interactive job or a job which writes
the output of that command to a file).

You will see many lines of output, but there should be a block that looks like the following:

.. code-block::

    1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN qlen 1
        link/loopback 00:00:00:00:00:00 brd 00:00:00:00:00:00
        inet 127.0.0.1/8 scope host lo
    3: eno49.4010@eno49: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP qlen 1000
        inet 10.XX.Y.Z/16 brd 10.XX.255.255 scope global eno49.4010
    4: eno49.4049@eno49: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP qlen 1000
        inet 198.XX.YYY.ZZZ/24 brd 198.XX.252.255 scope global eno49.4049

the ``XX``, ``YYY`` and ``ZZZ`` will have values corresponding to your cluster's configuration.
There are a few critical items:

- The headers (``lo``, ``eno.49...``, yours will be different) and the addresses where he ``XX`` placeholders are.
- Ignore the ``lo`` adapter, every machine should have one.
- The head node should have a ``inet`` that looks like a normal IP address, and another one which
  looks like it has a ``10.something`` IP address.
- The compute node will likely have an adapter which is only the ``10.something``.
- These ``10.something`` IP addresses are often intranet communication only, meaning the compute
  nodes cannot reach the broader internet

The name of the ethernet adapter housing
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

Replace the ``.XX.Y.Z`` with the code which has the intranet IP of the *head node*. This option
acts as a pass through to the Dask :term:`Worker` call and tells the worker to try and connect to the
head node at that IP address.

If that still doesn't work, contact us. We're working to make this less manual and difficult in the future.


Other variants:

- "My jobs start and stop instantly"
- "My jobs restart forever"


My Conda Environments are not Activating
++++++++++++++++++++++++++++++++++++++++

You likely have to ``source`` the Conda ``profile.d`` again first. See also
`<https://github.com/conda/conda/issues/8072>`_

This can also happen during testing where you will see command-line based binaries (like Psi4) pass, but Python-based
codes (like RDKit) fail saying complaining about an import error. On cluster compute nodes, this often manifests as
the ``$PATH`` variable being passed from your head node correctly to the compute node, but then the Python imports
cannot be found because the Conda environment is not set up correctly.

This problem is obfuscated by the fact that
:term:`workers<Worker>` such as Dask Workers can still start initially despite being a Python code themselves. Many
:term:`adapters<Adapter>` will start their programs using the absolute Python binary path which gets around the
incomplete Conda configuration. **We strongly recommend you do not try setting the absolute Python path** in your
scripts to get around this, and instead try to ``source`` the Conda ``profile.d`` first. For example, you might
need to add something like this to your YAML file (change paths/environment names as needed):

.. code-block:: yaml

    cluster:
        task_startup_commands:
            - source ~/miniconda3/etc/profile.d/conda.sh
            - conda activate qcfractal


Other variants:

- "Tests from one program pass, but others don't"
- "I get errors about unable to find program, but its installed"
- "I get path and/or import errors when testing"


My jobs appear to be running, but only one (or few) workers are starting
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

If the jobs appear to be running (and the Manager is reporting they return successfully),
a few things may be happening.

- If jobs are completing very fast, the :term:`Adapter` may not feel like it needs to start more
  :term:`workers<Worker>`, which is fine.
- (Not recommended, use for debug only) Check your ``manger.max_queued_tasks`` arg to pull more :term:`tasks<Task>`
  from the :term:`Server` to fill the jobs you have started. This option is usually automatically calculated based on
  your ``common.tasks_per_worker`` and ``common.max_workers`` to keep all :term:`workers<Worker>` busy and
  still have a buffer.
