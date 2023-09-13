QCFractalCompute - Compute Managers and Workers
===============================================

QCFractal executes quantum chemical calculations via *compute managers* deployed to resources suitable for these types of calculations.
This document illustrates how to 


Compute manager setup
---------------------
Create the manager ``qcfractal-manager-config.yml``::

    ---
    cluster: <cluster_name>
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
        workers_per_node: 4
        cores_per_worker: 16
        memory_per_worker: 96
        max_nodes: 2
        walltime: "4:00:00"
        project: null
        queue: <queue_name>
        request_by_nodes: false
        scheduler_options:
          - "-R fscratch"
        queue_tags:
          - '*'
        environments:
          use_manager_environment: False
          conda:
            - <worker_conda_env_name>
        worker_init:
          - source <absolute_path>/worker_init.sh


Create the file ``worker_init.sh``::



We will want to create a worker conda environment, for example an environment suitable for `psi4`_ execution, with an env file such as::

    name: qcaworker-psi4-18.1
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


.. _psi4: https://psicode.org/


Config reference for many HPC cluster types
-------------------------------------------
For other HPC clusters

.. autopydantic_model:: qcfractalcompute.config.LocalExecutorConfig
.. autopydantic_model:: qcfractalcompute.config.SlurmExecutorConfig
.. autopydantic_model:: qcfractalcompute.config.TorqueExecutorConfig
.. autopydantic_model:: qcfractalcompute.config.LSFExecutorConfig
