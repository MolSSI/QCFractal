Queue Manager Example YAML Files
================================

The primary way to set up a :term:`Manager` is to setup a YAML config file.
This page provides helpful config files which mostly can be just copied
and used in place (filling in things like :ref:`**username** and **password** <managers_server>`
as needed.)

The full documentation of every option and how it can be used can be found in
:doc:`the Queue Manager's API <managers_config_api>`.

For these examples, the ``username`` will always be "Foo" and the ``password`` will always be "b4R"
(which are just placeholders and not valid).

SLURM Cluster, Dask Adapter
----------------------------

To start a manager with a dask :term:`Adapter`, on a SLURM cluster, consuming 1 CPU and 8 GB of ram, targeting a Fractal
Server running on that cluster, and using the SLURM partition ``default``, save the following YAML config file:

.. code-block:: yaml

    common:
     adapter: dask
     ntasks: 1
     ncores: 1
     memory: 8

    server:
     fractal_uri: "api.qcarchive.molssi.org:443"
     username: Foo
     password: b4R

    manager:
     manager_name: "My First Manager"

    cluster:
     scheduler: slurm
     walltime: "72:00:00"

    dask:
     queue: default


Mutiple Tasks, 1 Cluster Job
----------------------------

This example starts a max of 1 cluster :term:`Job`, but multiple :term:`tasks<Task>`. The hardware will be
consumed uniformly by each :term:`Worker`. With 8 cores, 20 GB of memory, and 4 tasks; each :term:`Worker` will get
2 core and 5 GB of memory to work with. We set ``cluster.max_cluster_jobs`` to 1 to limit the number
of jobs which can be started. Since this is SLURM, the ``squeue`` information will show this
user has run 1 ``sbatch`` jobs which requested 4 cores and 20 GB of memory.

.. code-block:: yaml

    common:
     adapter: dask
     ntasks: 4
     ncores: 8
     memory: 20

    server:
     fractal_uri: "api.qcarchive.molssi.org:443"
     username: Foo
     password: b4R

    manager:
     manager_name: "A multi-task manager"

    cluster:
     scheduler: slurm
     walltime: "72:00:00"
     max_cluster_jobs: 1

    dask:
     queue: default


Testing the Manager Setup
-------------------------

This will test the :term:`Manager` to make sure its setup correctly, and does not need to
connect to the :term:`Server`, and therefore does not need a ``server`` block.

.. code-block:: yaml

    common:
     adapter: dask
     ntasks: 2
     ncores: 4
     memory: 10

    manager:
     manager_name: "A test manager"
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

An important note about this one, we have now set ``max_cluster_jobs`` to something larger than 1.
Each :term:`Job` will still request 4 cores and 256 GB of memory to be evenly distributed between the
4 :term:`tasks<Task>`, however, the :term:`Adapter` will **attempt to start be 5 independent** :term:`jobs<Job>`, for a
total of 80 cores, 1.280 TB of memory, distributed over 20 :term:`workers<Worker>`. If the :term:`Scheduler` does not
allow all of those :term:`jobs<Job>` to start, whether due to lack of resources or user limits, the
:term:`Adapter` can still start fewer :term:`jobs<Job>`, each with 16 cores, 256 GB of memory.

.. code-block:: yaml

    common:
     adapter: dask
     ntasks: 4
     ncores: 16
     memory: 256

    server:
     fractal_uri: api.qcarchive.molssi.org:443
     username: Foo
     password: b4R

    manager:
     manager_name: "Module Run Manager"
     test: False

    cluster:
     scheduler: sge
     task_startup_commands:
         - module load mpi/gcc/openmpi-1.6.4
         - conda activate qcfmanager
     walltime: "71:00:00"
     max_cluster_jobs: 5

    dask:
     queue: free64


Additional Scheduler Flags
--------------------------

A :term:`Scheduler` may ask you to set additional flags (or you might want to) when submitting a :term:`Job`.
Maybe its a Sys. Admin enforced rule, maybe you want to pull from a specific account, or set something not
interpreted for you in the :term:`Manager` or :term:`Adapter` (do tell us though if this is the case). This
example sets additional flags on a PBS cluster such that the final :term:`Job` file will have ``#PBS {my headers}``.
This example also uses Parsl and sets a scratch directory.

.. code-block:: yaml

    common:
     adapter: parsl
     ntasks: 1
     ncores: 6
     memory: 64
     scratch_directory: "$TMPDIR"

    server:
     fractal_uri: api.qcarchive.molssi.org:443
     username: Foo
     password: b4R
     verify: False

    manager:
     max_tasks: 10
     manager_name: "Options Manager"

    cluster:
     max_cluster_jobs: 5
     node_exclusivity: False
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
