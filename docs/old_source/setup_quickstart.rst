Setup Overview
==============

QCFractal comprises two components:

1. The :term:`Server` (``qcfractal-server``), which accepts compute and data queries and maintains a database of :term:`tasks <task>` and results. The :term:`Server` should be run continuously on a persistent machine.
2. One or more :term:`Managers <Manager>` (``qcfractal-manager``). The :term:`Managers <Manager>` pull work from the :term:`Server`, use attached compute resources to complete the work, and report results back to the server. :term:`Managers <Manager>` may be turned off and on at any time. :term:`Managers <Manager>` connect to compute resources through :term:`Adapters <Adapter>`.

In the :doc:`Quickstart Tutorial <quickstart>`, the above components were combined within a python environment using ``FractalSnowflake``.
In general, the :term:`Server` and :term:`Manager(s) <Manager>` are run separately in different processes, often on different machines.
For detailed information about the relationship between :term:`Server` and :term:`Manager`, see :doc:`managers`.


Common Use Cases
----------------

The table below lists some common use cases for QCFractal:

.. list-table::
   :widths: 25 25 25 25
   :header-rows: 1

   * - Use case
     - ``qcfractal-server`` location
     - ``qcfractal-manager`` location
     - Recommended manager
   * - :doc:`Demonstration/Exploration <quickstart>`
     - Snowflake
     - Snowflake
     - Snowflake
   * - :ref:`Single Workstation <quickstart-single-workstation>`
     - Local
     - Local
     - Pool
   * - :ref:`Private Cluster <quickstart-private-cluster>`
     - Head node
     - Head node
     - Parsl
   * - :ref:`Shared Cluster/Supercomputer <quickstart-shared-cluster>`
     - Personal server, head node (if permitted)
     - Head node
     - Parsl
   * - :ref:`Multiple Clusters <quickstart-shared-cluster>`
     - Personal server
     - Head node of each cluster
     - Parsl
   * - :ref:`Cloud Compute <quickstart-k8s>`
     - Personal server or cloud instance
     - Docker container
     - Pool

QCFractal is highly adaptable and is not limited to the above use cases.
For example, it possible to mix local, cluster, supercomputer, and cloud :term:`Managers <Manager>` simultaneously.
In addition, a cloud instance may provide a good option for running ``qcfractal-server`` when a persistent web-exposed server is not otherwise available.

Quickstart Setups
-----------------
This section presents quickstart setup guides for the above common use cases.
The guides assume that QCFractal has been installed (see :doc:`install`).
General guides are also available:

* :doc:`setup_server`
* :doc:`setup_compute`

.. _quickstart-single-workstation:

Single Workstation
++++++++++++++++++

This quickstart guide addresses QCFractal setup on a single computer which will be used for the :term:`Server`, :term:`Manager`, user client, and compute.
On the workstation, initialize the :term:`Server`::

   qcfractal-server init

Next, start the :term:`Server` and ProcessPoolExecutor :term:`Manager`::

   nohup qcfractal-server start --local-manager 1 &

The second command starts ``qcfractal-server`` in the background.
It also starts one :term:`Worker` which will pull :term:`tasks <Task>` from the :term:`Server` and run them.

Test if everything is setup by running a Hartree-Fock calculation on a single hydrogen molecule,
as in the :doc:`quickstart` (note this requires ``psi4``):

.. code-block:: python

   python

   >>> import qcfractal.interface as ptl

   # Note that server TLS verification is turned off (verify=False) since all components are run locally.
   >>> client = ptl.FractalClient(address="localhost:7777", verify=False)
   >>> mol = ptl.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0])
   >>> mol_id = client.add_molecules([mol])[0]
   >>> r = client.add_compute("psi4", "HF", "STO-3G", "energy", None, [mol_id])

   # Wait a minute for the job to complete
   >>> proc = client.query_procedures(id=r.ids)[0]
   >>> print(proc)
   <ResultRecord(id='0' status='COMPLETE')>
   >>> print(proc.properties.scf_total_energy)
   -0.6865598095254312


.. _quickstart-private-cluster:

Private Cluster
+++++++++++++++

This quickstart guide addresses QCFractal setup on a private cluster comprising a head node and compute nodes, with a :term:`Scheduler` such as SLURM, PBS, or Torque.
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ which may be installed with ``pip`` or ``conda``.

Begin by initializing the :term:`Server` on the cluster head node::

    qcfractal-server init

Next, start the :term:`Server` in the background::

   nohup qcfractal-server start &

The :term:`Manager` must be configured before use. Create a configuration file (e.g. in ``~/.qca/qcfractal/my_manager.yaml``) based on the following template:

.. code-block:: yaml

   common:
    adapter: parsl
    tasks_per_worker: 1
    cores_per_worker: 6
    memory_per_worker: 64
    max_workers: 5
    scratch_directory: "$TMPDIR"

   cluster:
    node_exclusivity: True
    scheduler: slurm

   parsl:
    provider:
     partition: CLUSTER
     cmd_timeout: 30

You may need to modify these values to match the particulars of your cluster. In particular:

* The ``scheduler`` and ``partition`` options should be set to match the details of your :term:`Scheduler` (e.g. SLURM, PBS, Torque).
* Options related to :term:`Workers <Worker>` should be set appropriately for the compute node on your cluster.
  Note that Parsl requires that full nodes be allocated to each :term:`Worker` (i.e. ``node_exclusivity: True``).

For more information on :term:`Manager` configuration, see :doc:`managers` and :doc:`managers_samples`.

Finally, start the :term:`Manager` in the background on the cluster head node::

    nohup qcfractal-manager --config-file <path to config YAML> --verify=False &

Note that TLS certificate verification is disabled (``--verify=False``) because the :term:`Manager` and :term:`Server` are both run on the head node.

Test if everything is setup by running a Hartree-Fock calculation on a single hydrogen molecule,
as in the :doc:`quickstart` (note this requires ``psi4``):

.. code-block:: python

   python

   >>> import qcfractal.interface as ptl

   # Note that server TLS verification is turned off (verify=False) since all components are run locally.
   >>> client = ptl.FractalClient(address="localhost:7777", verify=False)
   >>> mol = ptl.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0])
   >>> mol_id = client.add_molecules([mol])[0]
   >>> r = client.add_compute("psi4", "HF", "STO-3G", "energy", None, [mol_id])

   # Wait a minute for the job to complete
   >>> proc = client.query_procedures(id=r.ids)[0]
   >>> print(proc)
   <ResultRecord(id='0' status='COMPLETE')>
   >>> print(proc.properties.scf_total_energy)
   -0.6865598095254312


.. _quickstart-shared-cluster:

Shared Clusters, Supercomputers, and Multiple Clusters
++++++++++++++++++++++++++++++++++++++++++++++++++++++

This quickstart guide addresses QCFractal setup on one or more shared cluster(s).
The :term:`Server` should be set up on a persistent server for which you have permission to expose ports.
For example, this may be a dedicated webserver, the head node of a private cluster, or a cloud instance.
The :term:`Manager` should be set up on each shared cluster.
In most cases, the :term:`Manager` may be run on the head node;
contact your system administrator if you are unsure.
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ to be installed for the :term:`Manager`. It may be installed with ``pip`` or ``conda``.

Begin by initializing the :term:`Server` on your persistent server::

    qcfractal-server init

The QCFractal server receives connections from :term:`Managers <Manager>` and clients on TCP port 7777.
You may optionally specify the ``--port`` option to choose a custom port.
You may need to configure your firewall to allow access to this port.

Because the :term:`Server` will be exposed to the internet,
security should be enabled to control access.
Enable security by changing the YAML file (default: ``~/.qca/qcfractal/qcfractal_config.yaml``)
``fractal.security`` option to ``local``:

.. code-block:: diff

   - security: null
   + security: local

Start the :term:`Server`::

   nohup qcfractal-server start &

.. note::

    You may optionally provide a TLS certificate to enable host verification for the :term:`Server`
    using the ``--tls-cert`` and ``--tls-key`` options.
    If a TLS certificate is not provided, communications with the server will still be encrypted,
    but host verification will be unavailable
    (and :term:`Managers <Manager>` and clients will need to specify ``verify=False``).

Next, add users for admin, the :term:`Manager`, and a user
(you may choose whatever usernames you like)::

   qcfractal-server user add admin --permissions admin
   qcfractal-server user add manager --permissions queue
   qcfractal-server user add user --permissions read write compute

Passwords will be automatically generated and printed. You may instead specify a password with the ``--password`` option.
See :doc:`server_user` for more information.

:term:`Managers <Manager>` should be set up on each shared cluster.
In most cases, the :term:`Manager` may be run on the head node;
contact your system administrator if you are unsure.

The :term:`Manager` must be configured before use.
Create a configuration file (e.g. in ``~/.qca/qcfractal/my_manager.yaml``) based on the following template:

.. code-block:: yaml

   common:
    adapter: parsl
    tasks_per_worker: 1
    cores_per_worker: 6
    memory_per_worker: 64
    max_workers: 5
    scratch_directory: "$TMPDIR"

   cluster:
    node_exclusivity: True
    scheduler: slurm

   parsl:
    provider:
     partition: CLUSTER
     cmd_timeout: 30

You may need to modify these values to match the particulars of each cluster. In particular:

* The ``scheduler`` and ``partition`` options should be set to match the details of your :term:`Scheduler` (e.g. SLURM, PBS, Torque).
* Options related to :term:`Workers <Worker>` should be set appropriately for the compute node on your cluster.
  Note that Parsl requires that full nodes be allocated to each :term:`Worker` (i.e. ``node_exclusivity: True``).

For more information on :term:`Manager` configuration, see :doc:`managers` and :doc:`managers_samples`.

Finally, start the :term:`Manager` in the background on each cluster head node::

    nohup qcfractal-manager --config-file <path to config YAML> --fractal-uri <URL:port of Server> --username manager -password <password> &

If you did not specify a TLS certificate in the ``qcfractal-server start`` step, you will additionally need to specify ``--verify False`` in the above command.

Test if everything is setup by running a Hartree-Fock calculation on a single hydrogen molecule,
as in the :doc:`quickstart`
(note this requires ``psi4`` to be installed on at least one compute resource).
This test may be run from any machine.

.. code-block:: python

   python

   >>> import qcfractal.interface as ptl

   # Note that server TLS verification may need to be turned off if (verify=False).
   # Note that the Server URL and the password for user will need to be filled in.
   >>> client = ptl.FractalClient(address="URL:Port", username="user", password="***")
   >>> mol = ptl.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0])
   >>> mol_id = client.add_molecules([mol])[0]
   >>> r = client.add_compute("psi4", "HF", "STO-3G", "energy", None, [mol_id])

   # Wait a minute for the job to complete
   >>> proc = client.query_procedures(id=r.ids)[0]
   >>> print(proc)
   <ResultRecord(id='0' status='COMPLETE')>
   >>> print(proc.properties.scf_total_energy)
   -0.6865598095254312



.. _quickstart-k8s:

Cloud Compute
+++++++++++++

This quickstart guide addresses QCFractal setup using cloud resources for computation.
The :term:`Server` should be set up on a persistent server for which you have permission to expose ports.
For example, this may be a dedicated webserver, the head node of a private cluster, or a cloud instance.
The :term:`Manager` will be set up on a `Kubernetes <https://kubernetes.io/>`_ cluster as a
`Deployment <https://kubernetes.io/docs/concepts/workloads/controllers/deployment/>`_.

Begin by initializing the :term:`Server` on your persistent server::

    qcfractal-server init

The QCFractal server receives connections from :term:`Managers <Manager>` and clients on TCP port 7777.
You may optionally specify the ``--port`` option to choose a custom port.
You may need to configure your firewall to allow access to this port.

Because the :term:`Server` will be exposed to the internet,
security should be enabled to control access.
Enable security by changing the YAML file (default: ``~/.qca/qcfractal/qcfractal_config.yaml``)
``fractal.security`` option to ``local``:

.. code-block:: diff

   - security: null
   + security: local

Start the :term:`Server`::

   nohup qcfractal-server start &

.. note::

    You may optionally provide a TLS certificate to enable host verification for the :term:`Server`
    using the ``--tls-cert`` and ``--tls-key`` options.
    If a TLS certificate is not provided, communications with the server will still be encrypted,
    but host verification will be unavailable
    (and :term:`Managers <Manager>` and clients will need to specify ``verify=False``).

Next, add users for admin, the :term:`Manager`, and a user
(you may choose whatever usernames you like)::

   qcfractal-server user add admin --permissions admin
   qcfractal-server user add manager --permissions queue
   qcfractal-server user add user --permissions read write compute

Passwords will be automatically generated and printed. You may instead specify a password with the ``--password`` option.
See :doc:`server_user` for more information.

The :term:`Manager` will be set up on a `Kubernetes <https://kubernetes.io/>`_ cluster as a
`Deployment <https://kubernetes.io/docs/concepts/workloads/controllers/deployment/>`_, running
Docker images which each contain QCEngine, QCFractal, and relevant programs. In this guide,
we use the `molssi/qcarchive_worker_openff <https://cloud.docker.com/u/molssi/repository/docker/molssi/qcarchive_worker_openff>`_
Docker image. For execution, this image includes:

* `Psi4 <http://www.psicode.org>`_, `dftd3 <https://github.com/loriab/dftd3>`_, and `MP2D <https://github.com/Chandemonium/MP2D>`_
* `RDKit <https://www.rdkit.org>`_
* `geomeTRIC <https://github.com/leeping/geomeTRIC>`_


.. note::

    You may wish to set up a custom Docker image for your specific use case. The Dockerfile corresponding to the
    `molssi/qcarchive_worker_openff <https://cloud.docker.com/u/molssi/repository/docker/molssi/qcarchive_worker_openff>`_
    image is included below as an example.

    .. code-block:: Docker

        FROM continuumio/miniconda3
        RUN conda install -c psi4/label/dev -c conda-forge psi4 dftd3 mp2d qcengine qcfractal rdkit geometric
        RUN groupadd -g 999 qcfractal && \
            useradd -m -r -u 999 -g qcfractal qcfractal
        USER qcfractal
        ENV PATH /opt/local/conda/envs/base/bin/:$PATH
        ENTRYPOINT qcfractal-manager --config-file /etc/qcfractal-manager/manager.yaml

Create a manager configuration file (e.g. ``manager.yaml``) following the template below.

.. code-block:: yaml

    common:
     adapter: pool
     tasks_per_worker: 1
     cores_per_worker: 4  # CHANGEME number of cores/worker
     memory_per_worker: 16  # CHANGEME memory/worker in Gb
     max_workers: 1
     scratch_directory: "$TMPDIR"

    server:
     fractal_uri: api.qcarchive.molssi.org:443  # CHANGEME URI of your server goes here
     username: manager
     password: foo  # CHANGEME manager password goes here
     verify: True  # False if TLS was skipped earlier

    manager:
     manager_name: MyManager  # CHANGEME name your manager
     queue_tag: null
     log_file_prefix: null
     update_frequency: 30
     test: False

Add the manager configuration as a secret in Kubernetes::

    kubectl create secret generic manager-config-yaml --from-file=manager.yaml

This allows us to pass the manager configuration into the Docker container securely.

Next, create a Kubernetes deployment configuration file (e.g. ``deployment.yaml``) following the template below.
The ``cpu`` and ``memory`` fields of the deployment configuration should match the ``cores_per_worker``
and ``memory_per_worker`` fields of the manager configuration.
In this setup, ``replicas`` determines the number of workers; the ``max_workers`` and ``tasks_per_worker`` fields
in the manager configuration should be set to 1.

.. code-block:: yaml

    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: qcfractal-manager
      labels:
        k8s-app: qcfractal-manager
    spec:
      replicas: 4  # CHANGEME: number of images here
      selector:
        matchLabels:
          k8s-app: qcfractal-manager
      template:
        metadata:
          labels:
            k8s-app: qcfractal-manager
        spec:
          containers:
          - image: molssi/qcarchive_worker_openff  # you may wish to specify your own Docker image here
            name: qcfractal-manager-pod
            resources:
              limits:
                cpu: 4  # CHANGEME number of cores/worker
                memory: 16Gi  # CHANGEME memory/worker
            volumeMounts:
              - name: manager-config-secret
                mountPath: "/etc/qcfractal-manager"
                readOnly: true
          volumes:
            - name: manager-config-secret
              secret:
                secretName: manager-config-yaml

Start the deployment::

    kubectl apply -f deployment.yaml

.. note::

    You can view the status of your deployment with::

        kubectl get deployments

    You can view the status of individual "Pods" (Docker containers) with::

        kubectl get pods --show-labels

    To get the output of invidual Managers::

        kubectl logs <pod name>

    To get Kubernetes metadata and status information about a Pod::

        kubectl describe pod <pod name>

    See the `Kubernetes Deployment documentation <https://kubernetes.io/docs/concepts/workloads/controllers/deployment/>`_
    for more information.

Test if everything is setup by running a Hartree-Fock calculation on a single hydrogen molecule,
as in the :doc:`quickstart`
(note this requires ``psi4`` to be installed on at least one compute resource).
This test may be run from any machine.

.. code-block:: python

   python

   >>> import qcfractal.interface as ptl

   # Note that server TLS verification may need to be turned off if (verify=False).
   # Note that the Server URL and the password for user will need to be filled in.
   >>> client = ptl.FractalClient(address="URL:Port", username="user", password="***")
   >>> mol = ptl.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0])
   >>> mol_id = client.add_molecules([mol])[0]
   >>> r = client.add_compute("psi4", "HF", "STO-3G", "energy", None, [mol_id])

   # Wait a minute for the job to complete
   >>> proc = client.query_procedures(id=r.ids)[0]
   >>> print(proc)
   <ResultRecord(id='0' status='COMPLETE')>
   >>> print(proc.properties.scf_total_energy)
   -0.6865598095254312



Other Use Cases
---------------

QCFractal is highly configurable and supports many use cases beyond those described here.
For more information, see the :doc:`Server <server_init>` and :doc:`Manager <managers>` documentation sections.
You may also :ref:`contact us <work-with-us>`.
