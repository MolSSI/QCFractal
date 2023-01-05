Using datasets
==============

A *dataset* is a relatively homogeneous collection of records that allows for
submission and management of a large number of computations.

A dataset is made up of entries, specifications, and records.
It can be thought of as a table, where the *entries* are rows of the
table, and *specifications* are the columns. A cell within the table
(intersection between a row /entry and column/specification) is a :ref:`record <glossary_record>`.

Below is an example of this analogy, where the records are identified by their ID.
For example, record 18263 is an HF/sto-3g computation on water, and
record 23210 is an MP2/cc-pvdz computation on ethanol.

.. table::

  ==============  ==============  =================  =============
    Entry           HF/sto-3g      B3LYP/def2-tzvp    MP2/cc-pvdz
  ==============  ==============  =================  =============
   **water**          18263        18277              18295
   **methane**        19722        19642              19867
   **ethanol**        20212        20931              23210
  ==============  ==============  =================  =============

Using a dataset allows for control of entire rows and columns of the table, and even
the entire table itself. As an example, you can add a new specification, and then easily
submit computations for that specification that apply to all the existing entries.

Dataset Limitations
-------------------

A dataset can only contain one type of calculation. For example, you can have a :ref:`singlepoint_dataset`
or a :ref:`optimization_dataset`, but not a dataset that contains both single point and optimization
calculations.

A specification should work for all entries in the dataset. There is some limited ability to override
keywords on a per-entry basis, but there is no way to assign a different basis for a particular entry.


Retrieving datasets
-------------------

Datasets have unique ID, and a unique name. The unique name only applies to datasets of the same type,
so two datasets can have the same name as long as they are of different types. The names are not case sensitive.

You can retrieve a dataset with via its ID with :meth:`~qcportal.client.PortalClient.get_dataset_by_id`
and its name with :meth:`~qcportal.client.PortalClient.get_dataset`


.. tabs::
  .. tab:: QCPortal
    .. code-block:: py3

      >>> ds = client.get_dataset_by_id(123)
      >>> print(ds.id, ds.dataset_type, ds.name)
      123 singlepoint Organic molecule energies

      >>> ds = client.get_dataset("optimization", "Diatomic geometries")
      >>> print(ds.id, ds.dataset_type, ds.name)
      52 optimization Diatomic geometries



Adding datasets
---------------

Datasets can be created on a server with the :meth:`~qcportal.client.PortalClient.add_dataset`
function of the :class:`~qcportal.client.PortalClient`.
