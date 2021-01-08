Reaction Dataset
================

:class:`ReactionDatasets <qcportal.collections.ReactionDataset>` are useful for chemical reaction-based computations.
Currently, there are two types of :class:`ReactionDatasets <qcportal.collections.ReactionDataset>`:

- canonical chemical reaction datasets, ``rxn``: :math:`A + B \rightarrow C`,
- interaction energy datasets, ``ie``: :math:`M_{complex} \rightarrow M_{monomer_1} + M_{monomer_2}`.

Querying the Data
-----------------

The result specifications in a :class:`ReactionDataset <qcportal.collections.ReactionDataset>`
such as *method*, *basis set*, *program*, *keyword*, and *driver* may be listed with
:meth:`list_values <qcportal.collections.ReactionDataset.list_values>`.
In addition to the aforementioned specifications, :class:`ReactionDataset <qcportal.collections.ReactionDataset>` provides
a ``stoich`` field to select different strategies, including counterpoise-corrected (``"cp"``) and uncorrected (``"default"``),
for the calculation of interaction and reaction energies.

The computed values of the reaction properties such as interaction or reaction energies
can be queried using :meth:`get_values <qcportal.collections.ReactionDataset.get_values>`.
For results calculated with QCFractal, the underlying :class:`Records <qcportal.models.ResultRecord>`
can be retrieved with :meth:`get_records <qcportal.collections.ReactionDataset.get_records>` and further broken down by
:class:`Molecule <qcportal.models.Molecule>` within each reaction.

For examples of querying :class:`ReactionDatasets <qcportal.collections.ReactionDataset>`,
see the `QCArchive examples <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/reaction_datasets.html>`_.

Statistics and Visualization
----------------------------

Statistical analysis on the :class:`ReactionDatasets <qcportal.collections.ReactionDataset>` 
can be performed via :meth:`statistics <qcportal.collections.ReactionDataset.statistics>` command
which can be complemented by the :meth:`visualize <qcportal.collections.ReactionDataset.visualize>` 
command in order to plot the data. For examples pertinent to data visualization 
in :class:`ReactionDatasets <qcportal.collections.ReactionDataset>` see 
the `QCArchive examples <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/reaction_datasets.html>`_.

Creating the Datasets
---------------------

The :class:`Dataset <qcportal.collections.Dataset>` constructor can be adopted to create an empty dataset
object using a dataset name and type (``dtype``) as arguments.

.. code-block:: python

    >>> ds = ptl.collections.Dataset("my_dataset", dtype="rxn")

New reactions can be added to the :class:`ReactionDatasets <qcportal.collections.ReactionDataset>`
by providing a linear combination of :class:`Molecules <qcportal.models.Molecule>`
in order to compute the desired quantity. When the :class:`ReactionDataset <qcportal.collections.ReactionDataset>`
is queried, these linear combinations from chemical equations are automatically combined and presented to the caller.

.. code-block:: python

    >>> ds = ptl.collections.Dataset("Atomization Energies", dtype="ie")

    >>> N2 = ptl.Molecule.from_data("""
    >>> N 0.0 0.0 1.0975
    >>> N 0.0 0.0 0.0
    >>> unit angstrom
    >>> """)

    >>> N_atom = ptl.Molecule.from_data("""
    >>> 0 2
    >>> N 0.0 0.0 0.0
    >>> """)


    >>> ds.add_rxn("Nitrogen Molecule", [(N2, 1.0), (N_atom, -2.0)])

The details of a given chemical reaction can be obtained through using
:meth:`get_rxn() <qcportal.collections.ReactionDataset.get_rxn>` function.
The storage of ``molecule_hash`` in the :class:`ReactionDataset <qcportal.collections.ReactionDataset>`
is followed by that of the reaction coefficients.

.. code-block:: python

    >>> json.dumps(ds.get_rxn("Nitrogen Molecule"), indent=2)
    {
      "name": "Nitrogen Molecule",
      "stoichiometry": {
        "default": {
          "1": 1.0,
          "2": -2.0
        }
      },
      "attributes": {},
      "reaction_results": {
        "default": {}
      }
    }


Datasets of dtype ``ie`` can automatically construct counterpoise-corrected
(``cp``) or non-counterpoise-corrected (``default``) n-body expansions. The
the *key:value* pairs of numbers after the stoichiometry entry correspond to the
index of each atomic/molecular species and their corresponding number of moles
(or chemical equivalents) involved in the chemical reaction, respectively.

.. code-block:: python

    >>> ie_ds = ptl.collections.ReactionDataset("my_dataset", dtype="rxn")

    >>> water_dimer_stretch = ptl.data.get_molecule("water_dimer_minima.psimol")

    >>> ie_ds.add_ie_rxn("water dimer minima", water_dimer_stretch)

    >>> json.dumps(ie_ds.get_rxn("water dimer minima"), indent=2)
    {
      "name": "water dimer minima",
      "stoichiometry": {
        "default1": {  # Monomers
          "3": 1.0,
          "4": 1.0
        },
        "cp1": {  # Monomers
          "5": 1.0,
          "6": 1.0
        },
        "default": {  # Complex
          "7": 1.0
        },
        "cp": {  # Complex
          "7": 1.0
        }
      },
      "attributes": {},
      "reaction_results": {
        "default": {}
      }
    }


Computational Tasks
-------------------

Computations on the :class:`ReactionDatasets <qcportal.collections.ReactionDataset>`
are performed in the same manner as mentioned in :ref:`Dataset Documentation <dataset-computing>`
section.

API
---

.. autoclass:: qcportal.collections.ReactionDataset
    :members:
    :inherited-members:
