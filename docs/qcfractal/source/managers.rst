Fractal Queue Managers
======================

Queue Managers are the processes which interface with the Fractal Server and
clusters, supercomputers, and cloud resources to execute the tasks in the
Fractal Server. These managers pull compute :term:`tasks<Task>` from the
server, and then pass them to various distributed back ends for computation
for a variety of different needs. The architecture of the Fractal Server
allows many managers to be created in multiple physical locations. Currently,
QCFractal supports the following:

- `Pool` - A python `ProcessPoolExecutor` for computing tasks on a single machine (or node).
- `Dask <http://dask.pydata.org/en/latest/docs.html>`_ - A graph-based workflow engine for laptops and small clusters.
- `Parsl <http://parsl-project.org>`_ - High-performance workflows.
- `Fireworks <https://materialsproject.github.io/fireworks/>`_ - A asynchronous Mongo-based distributed queuing system.

These backends allow QCFractal to be incredibly elastic in utilized
computational resources, scaling from a single laptop to thousands of nodes on
physically separate hardware. Our end goal is to be able to setup a manager at
a physical site and allow it to scale up and down as its task queue requires
and execute compute over long periods of time (months) without intervention.

The Queue Manager's interactions with the Fractal Server, the Distributed Compute Engine, the physical Compute
Hardware, and the user are shown in the following diagram.

.. image:: media/QCFractalQueueManager.png
   :width: 800px
   :alt: Flowchart of what happens when a user starts a Queue Manager
   :align: center

The main goals of the Queue Manager is to reduce the user's level of expertise needed to start compute with Fractal and,
more importantly, to need as little manual intervention as possible to have persistent compute. Ideally, you start
the manager in a background process, and then leave it be while it checks in with the Fractal Server from time to time
to get :term:`tasks<Task>`, and pushes/pulls :term:`tasks<Task>` from the distributed :term:`Adapter` as need be.

The manager itself is a fairly lightweight process and consumes very little CPU power on its own. You should talk
with your Sys. Admins to see if you can run this on a head node first, but the Queue Manager itself will consume
less than 1% CPU we have found and virtually no RAM.

Queue Manager Quick Starts
--------------------------

For those who just want to get up and going, consider the following examples.

Laptop/Desktop Quick Boot
+++++++++++++++++++++++++

To get a Manager set up with defaults, running on local hardware, consuming local CPU and RAM, targeting a
Fractal Server running locally, run the following:

.. code-block:: bash

    $ qcfractal-manager


SLURM Cluster, Dask Adapter
+++++++++++++++++++++++++++

To start a manager with a dask :term:`Adapter`, on a SLURM cluster, consuming 1 CPU and 8 GB of ram, targeting a Fractal
Server running on that cluster, and using the SLURM partition ``default``, save the following YAML config file:

.. code-block:: yaml

    common:
     adapter: dask
     ntasks: 1
     ncores: 1
     memory: 8

    cluster:
     scheduler: slurm

    dask:
     queue: default

and then run the following command:

.. code-block:: bash

    $ qcfractal-manager --config-file="path/to/config.yaml"

replacing the ``config-file`` arg with the path to the file you saved.


Queue Manager CLI
-----------------

The CLI for the Fractal Queue Manager acts as an **option-specific** overwrite of the YAML file for various
options and therefore its flags can be set in tandem with the YAML. However, it does not have as extensive control as
the YAML file and so complex Managers (like those running Dask and Parsl) need to be setup in YAML.

In case this ever falls out of date, you can always run ``qcfractal-manager --help`` to get the most up-to-date
help block.

.. code-block::

    $ qcfractal-manager --help

    usage: qcfractal-manager [-h] [--config-file CONFIG_FILE] [--adapter ADAPTER]
                         [--ntasks NTASKS] [--ncores NCORES] [--memory MEMORY]
                         [--scratch-directory SCRATCH_DIRECTORY] [-v]
                         [--fractal-uri FRACTAL_URI] [-u USERNAME]
                         [-p PASSWORD] [--verify VERIFY]
                         [--max-tasks MAX_TASKS] [--manager-name MANAGER_NAME]
                         [--queue-tag QUEUE_TAG]
                         [--log-file-prefix LOG_FILE_PREFIX]
                         [--update-frequency UPDATE_FREQUENCY] [--test]
                         [--ntests NTESTS]

    A CLI for a QCFractal QueueManager with a ProcessPoolExecutor, Dask, or Parsl
    backend. The Dask and Parsl backends *requires* a config file due to the
    complexity of its setup. If a config file is specified, the remaining options
    serve as CLI overwrites of the config.

    optional arguments:
      -h, --help            show this help message and exit
      --config-file CONFIG_FILE

    Common Adapter Settings:
      --adapter ADAPTER     The backend adapter to use, currently only {'dask',
                            'parsl', 'pool'} are valid.
      --ntasks NTASKS       The number of simultaneous tasks for the executor to
                            run, resources will be divided evenly.
      --ncores NCORES       The number of process for the executor
      --memory MEMORY       The total amount of memory on the system in GB
      --scratch-directory SCRATCH_DIRECTORY
                            Scratch directory location
      -v, --verbose         Increase verbosity of the logger.

    FractalServer connection settings:
      --fractal-uri FRACTAL_URI
                            FractalServer location to pull from
      -u USERNAME, --username USERNAME
                            FractalServer username
      -p PASSWORD, --password PASSWORD
                            FractalServer password
      --verify VERIFY       Do verify the SSL certificate, turn off for servers
                            with custom SSL certificiates.

    QueueManager settings:
      --max-tasks MAX_TASKS
                            Maximum number of tasks to hold at any given time.
      --manager-name MANAGER_NAME
                            The name of the manager to start
      --queue-tag QUEUE_TAG
                            The queue tag to pull from
      --log-file-prefix LOG_FILE_PREFIX
                            The path prefix of the logfile to write to.
      --update-frequency UPDATE_FREQUENCY
                            The frequency in seconds to check for complete tasks.

    Optional Settings:
      --test                Boot and run a short test suite to validate setup
      --ntests NTESTS       How many tests per found program to run, does nothing
                            without --test set


Terminology
-----------

There are a number of terms which can overlap in due to the layers of abstraction and the type of software and hardware
the Queue Manager interacts with. To help with that, the pages in this section will use the following terminology.
Several pieces of software we interface with may have their own terms or the same term with different meaning, but
because one goal of the Manager is to abstract those concepts away as best it can, we choose the following set. If
you find something inappropriately labeled, unclear, or overloaded in any way, please raise an issue
`on GitHub <https://github.com/MolSSI/QCFractal/issues/new/choose>`_ and help us make it better!

An important note: Not all the concepts/mechanics of the :term:`Manager` and :term:`Adapter` are covered here by design!
There are several abstraction layers and mechanics which the user should never have to interact with or even be aware
of. However, if you feel something is missing, let us know!

.. glossary::

    Manager
        The Fractal Queue Manager (this section). The term "Manager" presented by itself refers to this object.

    Adapter
        The specific piece of software which accepts :term:`tasks<Task>` from the :term:`Manager` and sends them to the physical hardware. It
        is also the software which interacts with a cluster's :term:`Scheduler` to allocate said hardware and start
        :term:`Job`

    Distributed Compute Engine
        A more precise, although longer-winded, term for the :term:`Adapter`.

    Scheduler
        The software running on a cluster which users request hardware from to run computational :term:`tasks<Task>`,
        e.g. PBS, SLURM,
        LSF, SGE, etc. This, by itself, does not have any concept of the :term:`Manager` or even the :term:`Adapter`
        as both interface with it, not the other way around. Individual users' clusters may, and in most every case,
        will have a different configuration, even amongst the same governing software. Therefore, do not treat every
        Scheduler the same.

    Job
        The specific allocation of resources (CPU, Memory, wall clock, etc) provided by the :term:`Scheduler` to the
        :term:`Adapter`. This is identical to if you requested batch-like job on a cluster (e.g. though ``qsub`` or
        ``sbatch``), however, it is more apt to think of the resources allocated in this way as "resources to be
        distributed to the :term:`Task` by the :term:`Adapter`". Although a user running a :term:`Manager` will likely
        not directly interact with these, its important to track as these are what your :term:`Scheduler` is actually
        running and your allocations will be charged by.

    Task
        A single unit of compute as defined by the Fractal Server (i.e. the item which comes from the Task Queue). These
        tasks are preserved as they pass to the distributed compute engine and are what are presented to each distributed
        compute engine's :term:`Worker`\s to compute

    Worker
        The process executed from the :term:`Adapter` on the allocated hardware inside a :term:`Job`. This process
        receives the :term:`tasks<Task>` tracked by the :term:`Adapter` and is responsible for their execution. There may
        be multiple Workers within a single :term:`Job`, and the resources allocated for said :term:`Job` will be
        distributed by the :term:`Adapter` using whatever the :term:`Adapter` is configured to do. This is often uniform,
        but not always.

    Server
        The Fractal Server that the :term:`Manager` connects to. This is the source of the
        :term:`Task`\s which are pulled from and pushed to.

