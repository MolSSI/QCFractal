Dataset
=======

Datasets are useful for computing many methods for a single set of reactions where a reaction in a combination of molecules such as. There are currently two types of datasets:

 - `rxn` for datasets based on canonical chemical reactions :math:`A + B \rightarrow C`
 - `ie` for interaction energy datasets :math:`M_{complex} \rightarrow M_{monomer1} + M_{monomer2}`


Querying
--------

Visualizing
-----------

Creating
--------

Blank dataset can be constructed by choosing a dataset name and a dataset type (`dtype`)

.. code-block:: python

    ds = ptl.collection.Dataset("my_dataset", dtype="ie")

Molecules can be added by hash or object.

.. code-block:: python



    ds.add_rxn



Computing
---------
