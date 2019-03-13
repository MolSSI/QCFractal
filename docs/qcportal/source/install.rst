Install QCPortal
=================

You can install ``qcportal`` with ``conda``, with ``pip``, or by installing from source.

Conda
-----

You can install or update ``qcportal`` using `conda <https://www.anaconda.com/download/>`_::

    conda install qcportal -c conda-forge

The ``qcportal`` package is maintained on the
`conda-forge channel <https://conda-forge.github.io/>`_.


Pip
---

To install or update ``qcportal`` with ``pip``::

    pip install -U qcportal

Developer Install
-----------------

The QCPortal package is part of the QCFractal package and is the
``qcfractal.interface`` folder. To install QCFractal from source, clone the
repository from `GitHub <https://github.com/molssi/qcfractal>`_::

    git clone https://github.com/MolSSI/QCFractal.git
    cd qcfractal
    pip install -e .

A developer version of ``qcportal`` can then be imported as:

.. code-block:: python

    >>> import qcfractal.interface as ptl