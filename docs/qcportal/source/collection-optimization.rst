Optimization Dataset
====================
The :class:`OptimizationDataset <qcportal.collections.OptimizationDataset>` collection 
represents the results of geometry optimization calculations performed on 
a series of :class:`Molecules <qcportal.models.Molecule>`. The
:class:`OptimizationDataset <qcportal.collections.OptimizationDataset>` 
uses metadata specifications via 
:class:`Optimization Specification <qcportal.models.OptimizationSpecification>` and 
:class:`QCSpecification <qcportal.models.QCSpecification>` classes to manage 
parameters of the geometry optimizer and the underlying gradient 
calculation, respectively.

The existing :class:`OptimizationDataset <qcportal.collections.OptimizationDataset>` 
collections can be listed or selectively returned through
:meth:`FractalClient.list_collections("OptimizationDataset") <qcportal.FractalClient.list_collections>`
and :meth:`FractalClient.get_collection("OptimizationDataset", name) <qcportal.FractalClient.get_collection>`, respectively.

Querying the Data
-----------------

All available optimization data specifications can be listed via 

.. code-block:: python

    >>> ds.list_specifications()

function. In order to show the status of the optimization calculations
for a given set of specifications, one can use:

.. code-block:: python

    >>> ds.status(["default"])

For each :class:`Molecule <qcportal.models.Molecule>`, the number of
steps in a geometry optimization procedure can be queried through calling:

.. code-block:: python

    >>> ds.counts()

function. Individual :class:`OptimizationRecords <qcportal.models.OptimizationRecord>`
can be obtained using:

.. code-block:: python

    >>> ds.get_record(name="CCO-0", specification="default")


Statistics and Visualization
----------------------------

The trajectory of energy change during the course of geometry optimization
can be plotted by adopting :class:`qcportal.models.OptimizationRecord.show_history()`
function.

Creating the Datasets
---------------------

A new collection object for :class:`OptimizationDataset <qcportal.collections.OptimizationDataset>`
can be created using

.. code-block:: python

    >>> ds = ptl.collections.OptimizationDataset(name = "QM8-T", client=client)

Specific set of parameters for geometry optimization can be defined and 
added to the dataset as follows:

.. code-block:: python

    >>> spec = {'name': 'default',
    >>>         'description': 'Geometric + Psi4/B3LYP-D3/Def2-SVP.',
    >>>         'optimization_spec': {'program': 'geometric', 'keywords': None},
    >>>         'qc_spec': {'driver': 'gradient',
    >>>         'method': 'b3lyp-d3',
    >>>         'basis': 'def2-svp',
    >>>         'keywords': None,
    >>>         'program': 'psi4'}}

    >>>  ds.add_specification(**spec)

    >>>  ds.save()

:class:`Molecules <qcportal.models.Molecule>` can be added to the
:class:`OptimizationDataset <qcportal.collections.OptimizationDataset>`
as new entries for optimization via:

.. code-block:: python

     ds.add_entry(name, molecule)

When adding multiple entries of molecules, saving the dataset 
onto the server should be postponed until after all molecules are added:

.. code-block:: python

    >>> for name, molecule in new_entries:
    >>>     ds.add_entry(name, molecule, save=False)

    >>> ds.save()

Computational Tasks
-------------------

In order to run a geometry optimization calculation based on
a particular set of parameters (the default set in this case),
one can adopt the

.. code-block:: python

    >>> ds.compute(specification="default", tag="optional_tag")

function from :class:`OptimizationDataset <qcportal.collections.OptimizationDataset>` class.

API
---

.. autoclass:: qcportal.collections.OptimizationDataset
    :members:
    :inherited-members:
