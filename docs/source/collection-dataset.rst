Dataset
=======

Datasets are useful for computing many methods for a single set of reactions where a reaction in a combination of molecules such as. There are currently two types of datasets:

 - ``rxn`` for datasets based on canonical chemical reactions :math:`A + B \rightarrow C`
 - ``ie`` for interaction energy datasets :math:`M_{complex} \rightarrow M_{monomer_1} + M_{monomer_2}`


Querying
--------

Visualizing
-----------

Creating
--------

Blank dataset can be constructed by choosing a dataset name and a dataset type (``dtype``).

.. code-block:: python

    ds = ptl.collections.Dataset("my_dataset", dtype="rxn")

New reactions can be added by providing the linear combination of molecules
required to compute the desired quantity. When the Dataset is queried these
linear combinations area automatically combined for the caller.

.. code-block:: python

    ds = ptl.collections.Dataset("Atomization Energies", dtype="ie")

    N2 = ptl.Molecule("""
    N 0.0 0.0 1.0975
    N 0.0 0.0 0.0
    unit angstrom
    """)

    N_atom = ptl.Molecule("""
    0 2
    N 0.0 0.0 0.0
    """)


    ds.add_rxn("Nitrogen Molecule", [(N2, 1.0), (N_atom, -2.0)])

A given reaction can be examined by using the ``get_rxn`` function. We store
the ``molecule_hash`` followed by the coefficient.

.. code-block:: python

    json.dumps(ds.get_rxn("Nitrogen Molecule"), indent=2)
    {
      "name": "Nitrogen Molecule",
      "stoichiometry": {
        "default": {
          "4d7518cc2c741f2b5f48d7c16e2ad4c660e11890": 1.0,
          "636aa99f49b32dd81d7c8cb3741e16c632835cdf": -2.0
        }
      },
      "attributes": {},
      "reaction_results": {
        "default": {}
      }
    }


Datasets of dtype ``ie`` can be automatically contstruct counterpoise-correct
(``cp``) and non-counterpoise correct (``default``) n-body expansions. Where
the number after the name corresponds to the number of bodies involved in the
computation.

.. code-block:: python

    ie_ds = ptl.collections.Dataset("my_dataset", dtype="rxn")

    water_dimer_stretch = ptl.data.get_molecule("water_dimer_minima.psimol")
    ie_ds.add_ie_rxn("water dimer minima", water_dimer_stretch)

    json.dumps(ie_ds.get_rxn("water dimer minima"), indent=2)

    {
      "name": "water dimer minima",
      "stoichiometry": {
        "default1": {
          "4cd68e5dde15c19fc2f5101d5fc5f19ac8afbc9c": 1.0,
          "da635a2e012a9ea876ea54422256bd93124e4271": 1.0
        },
        "cp1": {
          "9299ecc50e018f60128845e9f14b803da641f816": 1.0,
          "0f6382da1b658b634a05bc7c7f65ad115328f06f": 1.0
        },
        "default": {
          "358ad4bb4620e35cec79b17ec0f40acae1a548cb": 1.0
        },
        "cp": {
          "358ad4bb4620e35cec79b17ec0f40acae1a548cb": 1.0
        }
      },
      "attributes": {},
      "reaction_results": {
        "default": {}
      }
    }


Computing
---------

API
---

.. autoclass:: qcfractal.interface.collections.Dataset
    :members: