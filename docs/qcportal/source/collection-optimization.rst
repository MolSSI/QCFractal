Optimization Dataset
====================
The :class:`OptimizationDataset <qcportal.collections.OptimizationDataset>` collection 
represents the results of geometry optimizations calculations performed on 
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

Querying
--------

List specifications:

.. code-block:: python

    ds.list_specifications()

Show status of calculations for a given specification:

.. code-block:: python

    ds.status(["default"])

The number of geometry steps for each molecule can be shown:

.. code-block:: python

    ds.counts()

Individual :class:`OptimizationRecords <qcportal.models.OptimizationRecord>` can be extracted:

.. code-block:: python

    ds.get_record(name="CCO-0", specification="default")


Visualizing
-----------

See :class:`qcportal.models.OptimizationRecord.show_history`.

Creating
--------

Create a new collection:

.. code-block:: python

    ds = ptl.collections.OptimizationDataset(name = "QM8-T", client=client)

Provide a specification:

.. code-block:: python

    spec = {'name': 'default',
            'description': 'Geometric + Psi4/B3LYP-D3/Def2-SVP.',
            'optimization_spec': {'program': 'geometric', 'keywords': None},
            'qc_spec': {'driver': 'gradient',
            'method': 'b3lyp-d3',
            'basis': 'def2-svp',
            'keywords': None,
            'program': 'psi4'}}
     ds.add_specification(**spec)
     ds.save()

Add molecules to optimize:

.. code-block:: python

     ds.add_entry(name, molecule)

If adding molecules in batches, you may wish to defer saving the dataset to the server until all molecules are added:

.. code-block:: python

    for name, molecule in new_entries:
        ds.add_entry(name, molecule, save=False)
    ds.save()

Computing
---------

.. code-block:: python

    ds.compute(specification="default", tag="optional_tag")


API
---

.. autoclass:: qcportal.collections.OptimizationDataset
    :members:
    :inherited-members:
