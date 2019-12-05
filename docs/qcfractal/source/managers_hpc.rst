Queue Managers for High-Performance Computing
=============================================

High-performance computing (HPC) clusters are designed to complete highly-parallel tasks in a short time.
Properly leverging such clusters requires utilizing large numbers of compute nodes at the same time,
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

The Parsl adapter deploys a single ``interchange'' per Job and uses the HPC systems's
MPI task launcher to deploy the workers.
The interchange will run on the login or batch node (depending on the cluster's configuration)
once the Job is started and will communicate to the workers using Parsl's ØMQ messaging protocol.
The QCFractal QueueManager will only need to Parsl interchange and not any of the workers.

See the `example page <manager_samples.html>` for details on how to configure Parsl for your system.
The configuration setting ``common.nodes_per_job`` defines the ability to make multi-node allocation
requests to a scheduler via an Adapter.

Many Nodes per Job, More Than one Node Per Application
------------------------------------------------------

The recommended configuration for using node-parallel tasks is to have a single QCFractal worker
running on the batch node, and using that worker to launch MPI tasks on the compute nodes.
The differentiating aspect of deploying multi-node tasks is that the QCFractal Worker and
QCEngine Python process will run on different nodes than the quantum chemistry code.

The Parsl implementation for multi-node jobs will a single worker and Parsl interchange
running on the login or batch node.
The worker will call the MPI launching system to place quantum-chemistry calculations on
to the compute nodes of the clusters and run many tasks in parallel.


