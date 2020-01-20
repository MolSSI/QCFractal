Configuration for High-Performance Computing
============================================

High-performance computing (HPC) clusters are designed to complete highly-parallel tasks in a short time.
Properly leveraging such clusters requires utilizing large numbers of compute nodes at the same time,
which requires special configurations for the QCFractal manager.
This part of the guide details several routes for configuring HPC clusters to use either large numbers
tasks that each use only a single node, or deploying a smaller number of tasks that use
multiple nodes.

*Note*: This guide is currently limited to using the Parsl adapter and contains some configuration
options which do not work with other adapters.

Many Nodes per Job, Single Node per Application
-----------------------------------------------

The recommended configuration for a QCFractal manager to use multi-node Jobs with
tasks limited to a single node is launch many workers for a single Job.

The Parsl adapter deploys a single ''manager'' per Job and uses the HPC system's
MPI task launcher to deploy the Parsl executors on to the compute nodes.
Each "executor" will run a single Python process per QCEngine worker and can run
more than one worker per node.
The ``manager`` will run on the login or batch node (depending on the cluster's configuration)
once the Job is started and will communicate to the workers using Parsl's ØMQ messaging protocol.
The QCFractal QueueManager will connect to the Parsl manager for each Job.

See the `example page <managers_samples.html>`_ for details on how to configure Parsl for your system.
The configuration setting ``common.nodes_per_job`` defines the ability to make multi-node allocation
requests to a scheduler via an Adapter.

Many Nodes per Job, More than One Node per Application
------------------------------------------------------

The recommended configuration for using node-parallel tasks is to have a single QCFractal worker
running on the batch node, and using that worker to launch MPI tasks on the compute nodes.
The differentiating aspect of deploying multi-node tasks is that the QCFractal Worker and
QCEngine Python process will run on different nodes than the quantum chemistry code.

The Parsl implementation for multi-node jobs will place a Parsl single executor and interchange
on the login/batch node.
The Parsl executor will launch a number of workers (as separate Python processes)
equal to the number of nodes per Job divided by the number of nodes per Task.
The worker will call the MPI launch system to place quantum-chemistry calculations on
the compute nodes of the clusters.

See the `example page <managers_samples.html>`_ for details on how to configure Parsl for your system.
