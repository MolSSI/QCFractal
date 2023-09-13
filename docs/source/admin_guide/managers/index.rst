QCFractalCompute - Compute Managers and Workers
===============================================

QCFractal executes quantum chemical calculations via **compute managers** deployed to resources suitable for these types of calculations.
This document illustrates how to set up and run a compute manager on HPC resources.

Compute manager setup for an HPC cluster
----------------------------------------

.. note:: 
   The instructions in this section are performed on the head/login node of your HPC cluster.
   By the end, you will start up a compute manager on the head node, and it will launch jobs on your behalf.
   If your cluster admin forbits long-running processes on the head node, then refer to :ref:`compute-manager-local`.

Install the base environment for the compute manager, using `mamba`_, and activate it::

    $ mamba create -n qcfractalcompute -c conda-forge qcfractalcompute
    $ mamba activate qcfractalcompute

This will create the conda environment ``qcfractalcompute``, which the compute manager will run under.

Next, create the manager config as ``qcfractal-manager-config.yml``, using the content below as your starting point.
Fill in the components given in ``<brackets>``.
For this example, we are assuming a cluster using LSF as the scheduler; for other scheduler types, see :ref:`compute-manager-hpc-config-reference`::

    # qcfractal-manager-config.yml
    ---
    cluster: <cluster_name>           # descriptive name to present to QCFractal server
    loglevel: INFO
    logfile: qcfractal-manager.log
    update_frequency: 60.0
    
    server:
      fractal_uri: <fractal_url>      # e.g. https://qcarchive.molssi.org
      username: <compute_identity>
      password: <compute_key>
      verify: True
    
    executors:
      cpuqueue:
        type: lsf
        workers_per_node: 4           # max number of workers to spawn per node
        cores_per_worker: 16          # cores per worker
        memory_per_worker: 96         # memory per worker, in GiB
        max_nodes: 2                  # max number of jobs to have queued/running at a time
        walltime: "4:00:00"           # walltime, given in `hours:min:seconds`, alternatively in integer minutes
        project: null
        queue: <queue_name>           # name of queue to launch to
        request_by_nodes: false
        scheduler_options:            # add additional options for submission command
          - "-R fscratch"
        queue_tags:                   # only claim tasks with these tags; '*' means all tags accepted
          - '*'
        environments:
          use_manager_environment: False   # don't use the manager environment for task execution
          conda:
            - <worker_conda_env_name>      # name of conda env used for task execution; see below for example
        worker_init:
          - source <absolute_path>/worker-init.sh   # initialization script for worker; see below for example


Create the file ``worker-init.sh``; this is run before worker startup::

    # worker-init.sh
    #!/bin/bash
    
    # Make sure to run bashrc
    source $HOME/.bashrc
    
    # Don't limit stack size
    ulimit -s unlimited
    
    # make scratch space
    # CUSTOMIZE FOR YOUR CLUSTER
    mkdir -p /fscratch/${USER}/${LSB_JOBID}
    cd /fscratch/${USER}/${LSB_JOBID}
    
    # Activate qcfractalcompute conda env
    conda activate qcfractalcompute

Set the absolute path to this file in the line ``- source <absolute_path>/worker-init.sh`` in ``qcfractal-manager-config.yml`` above.

We will also create a conda environment used by each worker to execute tasks, e.g. an environment suitable for using `psi4`_, with a conda environment file such as ``worker-env.yml``::

    # worker-env.yml
    ---
    name: qcfractal-worker-psi4-18.1
    channels:
      - conda-forge/label/libint_dev
      - conda-forge
      - defaults
    dependencies:
      - python =3.10
      - pip
      - qcengine
      - psi4 =1.8.1
      - dftd3-python
      - gcp-correction
      - geometric
      - scipy

      - pip:
        - basis_set_exchange

And creating a conda environment from it with `mamba`_::

    $ mamba env create -f worker-env.yml

Set the name of this conda env (``qcfractal-worker-psi4-18.1``) in the line ``- <worker_conda_env_name>`` in ``qcfractal-manager-config.yml`` above.

Finally, start up the compute manager::

    $ qcfractal-compute-manager --config config.yml

The compute manager will read its config file, communicate with the QCFractal server to claim tasks, and launch jobs to the HPC scheduler as needed to execute those tasks using the worker conda environment.
To keep it running beyond your current session if connected via SSH, consider running the compute manager under `tmux`_ or `screen`_.

.. _mamba: https://mamba.readthedocs.io/en/latest/mamba-installation.html#mamba-installation
.. _psi4: https://psicode.org/
.. _tmux: https://github.com/tmux/tmux/wiki
.. _screen: https://en.wikipedia.org/wiki/GNU_Screen


.. _compute-manager-hpc-config-reference:

Configuration for different HPC schedulers 
------------------------------------------
HPC cluster schedulers vary in behavior, so you will need to adapt your ``qcfractal-manager-config.yml`` to the scheduler of the HPC cluster you intend to use.
The configuration keys available for each ``type`` of record in the ``executors`` list are referenced here.

----

.. autopydantic_model:: qcfractalcompute.config.SlurmExecutorConfig
   :model-show-config-summary: false
   :model-show-field-summary: false

----

.. autopydantic_model:: qcfractalcompute.config.TorqueExecutorConfig
   :model-show-config-summary: false
   :model-show-field-summary: false

----

.. autopydantic_model:: qcfractalcompute.config.LSFExecutorConfig
   :model-show-config-summary: false
   :model-show-field-summary: false

----

.. _compute-manager-local:

Execution without interfacing with an HPC scheduler
---------------------------------------------------
When running with a configuration like that above, the compute manager must remain alive on the head/login node of the cluster in order to execute tasks.
If leaving a long-running process running on the head node is undesirable, then consider using a ``local`` executor configuration instead, replacing the ``executors`` section in ``qcfractal-manager-config.yml`` with e.g.::

    executors:
      local_executor:
        type: local
        max_workers: 4                # max number of workers to spawn
        cores_per_worker: 16          # cores per worker
        memory_per_worker: 96         # memory per worker, in GiB
        queue_tags:
          - '*'
        environments:
          use_manager_environment: False
          conda:
            - <worker_conda_env_name>      # name of conda env used by worker; see below for example
        worker_init:
          - source <absolute_path>/worker_init.sh


You will then need to create a submission script suitable for your HPC scheduler that requests the appropriate resources, activates the ``qcfractalcompute`` conda environment, and runs ``qcfractal-compute-manager --config qcfractal-manager-config.yml`` itself.
You can then manually submit jobs using this script as needed to complete tasks available on the QCFractal server.

Using the ``local`` executor type is also recommended for running a compute manager on a standalone host, or within a container on e.g. a Kubernetes cluster.

----

.. autopydantic_model:: qcfractalcompute.config.LocalExecutorConfig
   :model-show-config-summary: false
   :model-show-field-summary: false
