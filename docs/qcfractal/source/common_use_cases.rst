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
   * - :ref:`Single workstation <single-workstation-quickstart>`
     - Local
     - Local
     - Pool
   * - :ref:`Private cluster <private-cluster-quickstart>`
     - Head node
     - Head node
     - Parsl
   * - :ref:`Shared Cluster/Supercomputer <shared-cluster-quickstart>`
     - Personal server, head node (if permitted)
     - Head node
     - Parsl
   * - :ref:`Multiple Clusters <multiple-clusters-quickstart>`
     - Personal server
     - Head node of each cluster
     - Parsl
   * - :ref:`Cloud <cloud-quickstart>`
     - Cloud instance
     - Cloud instance
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

.. _single-workstation-quickstart:

Single Workstation
++++++++++++++++++

This quickstart guide addresses QCFractal setup on a single computer which will be used for the :term:`Server`, :term:`Manager`, user client, and compute. 
On the workstation, setup and start the :term:`Server`::

   qcfractal-server init 
   nohup qcfractal-server start &

The second command starts ``qcfractal-server`` in the background. 

Next, start the local ProcessPoolExecutor :term:`Manager`::

   nohup qcfractal-manager --adapter pool --verify False &

This command starts one :term:`Worker` which will pull :term:`tasks <Task>` from the :term:`Server` and run them. Note that authentication is turned off (``--verify False``) since all components are run locally.

Test if the everything is setup by running a Hartee-Fock calculation a single hydrogen molecule, as in the :doc:`quickstart` (note this requires `psi4`)::

   python
   >>> import qcfractal.interface as ptl
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


.. _private-cluster-quickstart:

Private Cluster
+++++++++++++++

This quickstart guide addresses QCFractal setup on a private cluster comprising a head node and compute nodes, with a scheduler such as SLURM, PBS, or Torque. 
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ which may be installed with ``pip``.

.. _shared-cluster-quickstart:

Shared Cluster
++++++++++++++
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ to be installed on the head node of the shared cluster. It may be installed with ``pip``.

.. _multiple-clusters-quickstart:

Multiple Clusters
+++++++++++++++++
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ to be installed on the head nodes of each cluster. It may be installed with ``pip``.

.. _cloud-quickstart:

Cloud
+++++
This guide requires `Parsl <https://parsl.readthedocs.io/en/stable/quickstart.html>`_ which may be installed with ``pip``.
