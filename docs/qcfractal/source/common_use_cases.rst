Setup Overview and Quickstart
=============================

QCFractal comprises two components:

1. The :term:`Server` (``qcfractal-server``), which accepts compute and data queuries and maintains a database of :term:`tasks <task>` and results. The :term:`Server` should be run continuously on a persistant machine. 
2. One or more :term:`Managers <Manager>` (``qcfractal-manager``). The Managers pull work from the :term:`Server`, use attached compute resources to complete the work, and report results back to the server. :term:`Managers <Manager>` may be turned off and on at any time. :term:`Managers <Manager>` connect to compute resource through :term:`Adapters <Adapter>`.

In the :doc:`Quickstart Tutorial <quickstart>`, the above components were comined within a python envionment using `FractalSnowflake`. 
In general, the :term:`Server` and :term:`Manager(s) <Manager>` are run seapately, on separate machines.
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
   * - :ref:`Single workstation <quickstart-single-workstation>`
     - Local
     - Local
     - Pool
   * - :ref:`Private cluster <quickstart-private-cluster>`
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

QCFractal is highly adaptable and is not limited to the above use cases. 
For example, it possible to mix local, cluster, supercomputer, and cloud :term:`Managers <Manager>`. 
In addition, a cloud instance may provide a good option for running ``qcfractal-server`` when a persistent web-exposed server is not otherwise available. 

Quickstart Setups
-----------------
This section presents quickstart setup guides for the above common use cases.
The guides assume that QCFractal has been installed (see :doc:`install`).
More detailed guides are available:

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

Finally, :ref:`test your setup. <quickstart-test>`

.. _quickstart-private-cluster:

Private Cluster
+++++++++++++++

This quickstart guide addresses QCFractal setup on a private cluster comprising a head node and compute nodes, with a :term:`Scheduler` such as SLURM, PBS, or Torque. 
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ which may be installed with ``pip``.

Begin by initializing the :term:`Server` on the cluster head node::

    qcfractal-server init

Next, start the :term:`Server` in the background::

   nohup qcfractal-server start &

The :term:`Manager` must be configured before use. Create a configuration file (e.g. in ``~/.qca/qcfractal/my_manager.yaml``) based on the following template::

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

You may need to modify these values to match the particulars on your cluster. In particular:

* The `scheduler` and `partition` options should be set to match the details of your :term:`Scheduler` (e.g. SLURM, PBS, Torque).
* Options related to workers should be set appropriately for the compute node on your cluster. 
  Note that Parsl requires that full nodes be allocated to each worker (i.e. ``node_exclusivity: True``).

For more information on :term:`Manager` configuration, see :ref:`managers` and :ref:`managers_samples`.

Finally, start the :term:`Manager` in the background on the cluster head node::

    nohup qcfractal-manager --config-file <path to config YAML> --verify=False

Note that TLS certificate verification is disabled (``--verify=False``) because the :term:`Manager` and :term:`Server` are both run on the head node.

Finally, :ref:`test your setup. <quickstart-test>`

.. _quickstart-shared-cluster:

Shared Clusters, Supercomputers, and Multiple Clusters
++++++++++++++++++++++++++++++++++++++++++++++++++++++

This quickstart guide addresses QCFractal setup on one or more shared cluster. 
The :term:`Server` should be set up on a persistant server for which you have permission to expose ports. 
For example, this may be a dedicated webserver, the head node of a private cluster, or a cloud instance.
The :term:`Manager` should be set up on each shared cluster. 
In most cases, the :term:`Manager` may be run on the head node; 
contact your system administrator if you are unsure.
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ to be installed for the :term:`Manager`. It may be installed with ``pip``.

Begin by initializing the :term:`Server` on your persistant server::

    qcfractal-server init 

The QCFractal server recieves connections from :term:`Managers <Manager>` and clients on TCP port 7777. 
You may optionally specify the ``--port`` option to choose a custom port. 
You may need to configure your firewall to allow access to this port.

Because the :term:`Server` will be exposed to the internet, 
security should be enabled to control access. 
Enable security by changing the YAML file (default: ``~/.qca/qcfractal/qcfractal_config.yaml``)
``fractal.security`` option to ``local``::

   - security: null
   + security: local

Start the :term:`Server`::

   nohup qcfractal-server start &

You may optionally provide a TLS cerficiate to enable host verification for the :term:`Server` 
using the ``--tls-cert`` and ``--tls-key`` options. 
If a TLS certificate is not provided, communications with the server will still be encrypted, 
but host verification will be unavailable 
(and :term:`Managers <Manager>` and clients will need to specify ``--verify False``).

Next, add users for admin, the :term:`Manager`, and a user 
(you may choose whatever usernames you like)::

   qcfractal-server user add admin --permissions admin
   qcfractal-server user add manager --permissions queue
   qcfractal-server user add user --permissions read write compute

Passwords will be automatically generated and printed. You may instead specify a password with the ``--password`` option. 
See :doc:`server_user` for more information.

    qcfractal-manager --config-file ~/.qca/qcfractal/manager.yaml --fractal-uri URL:port --username manager -password 

Finally, :ref:`test your setup. <quickstart-test>`

.. _quickstart-test:

Test
++++
Test if the everything is setup by running a Hartee-Fock calculation a single hydrogen molecule, as in the :doc:`quickstart` (note this requires ``psi4``)::

   python
   >>> import qcfractal.interface as ptl
   # Note that server TLS verification is turned off (verify=False) since all components are run locally.
   >>> client = ptl.FractalClient(address="localhost:7777", verify=False)
   >>> mol = ptl.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0])
   >>> mol_id = client.add_molecules([mol])[0]
   >>> r = client.add_compute("psi4", "HF", "STO-3G", "energy", None, [mol_id])
   >>> # Wait a minute for the job to complete
   >>> proc = client.query_procedures(id=r.ids)[0]
   >>> print(proc)
   <ResultRecord(id='0' status='COMPLETE')>
   >>> print(proc.properties.scf_total_energy)
   -0.6865598095254312 

