Reaction Dataset
================

:class:`ReactionDatasets <qcportal.collections.ReactionDataset>` are useful for computing many methods for a set of reactions.
There are currently two types of :class:`ReactionDatasets <qcportal.collections.ReactionDataset>`:

- ``rxn`` for datasets based on canonical chemical reactions :math:`A + B \rightarrow C`
- ``ie`` for interaction energy datasets :math:`M_{complex} \rightarrow M_{monomer_1} + M_{monomer_2}`

Querying
--------

Available result specifications (method, basis set, program, keyword, driver combinations) in a
:class:`ReactionDataset <qcportal.collections.ReactionDataset>` may be listed with
:meth:`list_values <qcportal.collections.ReactionDataset.list_values>`.
Beyond those specifications in :class:`Datasets <qcportal.collections.Dataset>`,
:class:`ReactionDatasets <qcportal.collections.ReactionDataset>` provide a `stoich` field which may be used to
select different strategies for computation of interaction and reaction energies. By default, the counterpoise-corrected
(``"cp"``) and uncorrected (``"default"``) values are available.

Reaction values, such as interaction or reaction energies,
are queried with :meth:`get_values <qcportal.collections.ReactionDataset.get_values>`.
For results computed using QCFractal, the underlying :class:`Records <qcportal.models.ResultRecord>`
are retrieved with :meth:`get_records <qcportal.collections.ReactionDataset.get_records>`, and are broken down by
:class:`Molecule <qcportal.models.Molecule>` within the reaction.

For examples of querying :class:`ReactionDatasets <qcportal.collections.ReactionDataset>`,
see the `QCArchive examples <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/reaction_datasets.html>`_.

Visualizing
-----------

Statistics on :class:`ReactionDatasets <qcportal.collections.ReactionDataset>` may be computed using the
:meth:`statistics <qcportal.collections.ReactionDataset.statistics>` command,
and plotted using the :meth:`visualize <qcportal.collections.ReactionDataset.visualize>` command.

For examples of visualizing :class:`ReactionDatasets <qcportal.collections.ReactionDataset>`,
see the `QCArchive examples <https://qcarchivetutorials.readthedocs.io/en/latest/basic_examples/reaction_datasets.html>`_.


Creating
--------

An empty dataset can be constructed by choosing a dataset name and a dataset type (``dtype``).

.. code-block:: python

    ds = ptl.collections.Dataset("my_dataset", dtype="rxn")

New reactions can be added by providing the linear combination of :class:`Molecules <qcportal.models.Molecule>`
required to compute the desired quantity. When the :class:`ReactionDataset <qcportal.collections.ReactionDataset>` is queried these
linear combinations are automatically combined for the caller.

.. code-block:: python

    ds = ptl.collections.Dataset("Atomization Energies", dtype="ie")

    N2 = ptl.Molecule.from_data("""
    N 0.0 0.0 1.0975
    N 0.0 0.0 0.0
    unit angstrom
    """)

    N_atom = ptl.Molecule.from_data("""
    0 2
    N 0.0 0.0 0.0
    """)


    ds.add_rxn("Nitrogen Molecule", [(N2, 1.0), (N_atom, -2.0)])

A given reaction can be examined by using the :meth:`get_rxn <qcportal.collections.ReactionDataset.get_rxn>` function.
We store the ``molecule_hash`` followed by the reaction coefficient.

.. code-block:: python

    json.dumps(ds.get_rxn("Nitrogen Molecule"), indent=2)
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


Datasets of dtype ``ie`` can automatically construct counterpoise-correct
(``cp``) and non-counterpoise-correct (``default``) n-body expansions. The
the number after the stoichiometry corresponds to the number of bodies involved in the
computation.

.. code-block:: python

    ie_ds = ptl.collections.ReactionDataset("my_dataset", dtype="rxn")

    water_dimer_stretch = ptl.data.get_molecule("water_dimer_minima.psimol")
    ie_ds.add_ie_rxn("water dimer minima", water_dimer_stretch)

    json.dumps(ie_ds.get_rxn("water dimer minima"), indent=2)

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


Computing
---------

Computations are performed in the same manner as for a :class:`Dataset <qcportal.collections.Dataset>`.
See the :ref:`Dataset Documentation <dataset-computing>` for more information.

API
---

.. autoclass:: qcportal.collections.ReactionDataset
    :members:
    :inherited-members:
