Queue Manager Example YAML Files
================================

The primary way to set up a :term:`Manager` is to setup a YAML config file.
This page provides helpful config files which mostly can be just copied
and used in place (filling in things like :ref:`**username** and **password** <managers_server>`
as needed.)

The full documentation of every option and how it can be used can be found in
:doc:`the Queue Manager's API <managers_config_api>`.

For these examples, the ``username`` will always be "Foo" and the ``password`` will always be "b4R"
(which are just placeholders and not valid). The ``manager_name`` variable can be any string and these examples provide
some descriptive samples. The more distinct the name, the better it is to see its status on the :term:`Server`.

SLURM Cluster, Dask Adapter with additional options
---------------------------------------------------

This example is similar to the :ref:`example on the start page for Managers<manager_starter_example>`, but with some
additional options such as connecting back to a central Fractal instance and setting more cluster-specific options.
Again, this starts a manager with a dask :term:`Adapter`, on a SLURM cluster, consuming 1 CPU and 8 GB of ram, targeting
a Fractal Server running on that cluster, and using the SLURM partition ``default``, save the following YAML config
file:

.. code-block:: yaml

    common:
     adapter: dask
     tasks_per_worker: 1
     cores_per_worker: 1
     memory_per_worker: 8

    server:
     fractal_uri: "localhost:7777"
     username: Foo
     password: b4R

    manager:
     manager_name: "SlurmCluster_OneDaskTask"

    cluster:
     scheduler: slurm
     walltime: "72:00:00"

    dask:
     queue: default


Multiple Tasks, 1 Cluster Job
-----------------------------

This example starts a max of 1 cluster :term:`Job`, but multiple :term:`tasks<Task>`. The hardware will be
consumed uniformly by the :term:`Worker`. With 8 cores, 20 GB of memory, and 4 tasks; the :term:`Worker` will provide
2 cores and 5 GB of memory to compute each :term:`Task`. We set ``common.max_workers`` to 1 to limit the number
of :term:`Workers<Worker>` and :term:`Jobs <Job>` which can be started. Since this is SLURM, the ``squeue`` information
will show this user has run 1 ``sbatch`` jobs which requested 4 cores and 20 GB of memory.

.. code-block:: yaml

    common:
     adapter: dask
     tasks_per_worker: 4
     cores_per_worker: 8
     memory_per_worker: 20
     max_workers: 1

    server:
     fractal_uri: "localhost:7777"
     username: Foo
     password: b4R

    manager:
     manager_name: "SlurmCluster_MultiDask"

    cluster:
     scheduler: slurm
     walltime: "72:00:00"

    dask:
     queue: default


Testing the Manager Setup
-------------------------

This will test the :term:`Manager` to make sure it's setup correctly, and does not need to
connect to the :term:`Server`, and therefore does not need a ``server`` block. It will still however submit
:term:`jobs <Job>`.

.. code-block:: yaml

    common:
     adapter: dask
     tasks_per_worker: 2
     cores_per_worker: 4
     memory_per_worker: 10

    manager:
     manager_name: "TestBox_NeverSeen_OnServer"
     test: True
     ntests: 5

    cluster:
     scheduler: slurm
     walltime: "01:00:00"

    dask:
     queue: default


Running commands before work
----------------------------

Suppose there are some commands you want to run *before* starting the :term:`Worker`, such as starting a Conda
environment, or setting some environment variables. This lets you specify that. For this, we will run on a
Sun Grid Engine (SGE) cluster, start a conda environment, and load a module.

An important note about this one, we have now set ``max_workers`` to something larger than 1.
Each :term:`Job` will still request 16 cores and 256 GB of memory to be evenly distributed between the
4 :term:`tasks<Task>`, however, the :term:`Adapter` will **attempt to start 5 independent** :term:`jobs<Job>`, for a
total of 80 cores, 1.280 TB of memory, distributed over 5 :term:`Workers<Worker>` collectively running 20 concurrent
:term:`tasks<Task>`. If the :term:`Scheduler` does not
allow all of those :term:`jobs<Job>` to start, whether due to lack of resources or user limits, the
:term:`Adapter` can still start fewer :term:`jobs<Job>`, each with 16 cores and 256 GB of memory, but :term:`Task`
concurrency will change by blocks of 4 since the :term:`Worker` in each :term:`Job` is configured to handle 4
:term:`tasks<Task>` each.

.. code-block:: yaml

    common:
     adapter: dask
     tasks_per_worker: 4
     cores_per_worker: 16
     memory_per_worker: 256
     max_workers: 5

    server:
     fractal_uri: localhost:7777
     username: Foo
     password: b4R

    manager:
     manager_name: "GridEngine_OpenMPI_DaskWorker"
     test: False

    cluster:
     scheduler: sge
     task_startup_commands:
         - module load mpi/gcc/openmpi-1.6.4
         - conda activate qcfmanager
     walltime: "71:00:00"

    dask:
     queue: free64


Additional Scheduler Flags
--------------------------

A :term:`Scheduler` may ask you to set additional flags (or you might want to) when submitting a :term:`Job`.
Maybe it's a Sys. Admin enforced rule, maybe you want to pull from a specific account, or set something not
interpreted for you in the :term:`Manager` or :term:`Adapter` (do tell us though if this is the case). This
example sets additional flags on a PBS cluster such that the final :term:`Job` launch file will have
``#PBS {my headers}``.

This example also uses Parsl and sets a scratch directory.

.. code-block:: yaml

    common:
     adapter: parsl
     tasks_per_worker: 1
     cores_per_worker: 6
     memory_per_worker: 64
     max_workers: 5
     scratch_directory: "$TMPDIR"

    server:
     fractal_uri: localhost:7777
     username: Foo
     password: b4R
     verify: False

    manager:
     manager_name: "PBS_Parsl_MyPIGroupAccount_Manger"

    cluster:
     node_exclusivity: True
     scheduler: pbs
     scheduler_options:
         - "-A MyPIsGroupAccount"
     task_startup_commands:
         - conda activate qca
         - cd $WORK
     walltime: "06:00:00"

    parsl:
     provider:
      partition: normal_q
      cmd_timeout: 30
