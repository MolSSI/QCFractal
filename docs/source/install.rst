Install QCFractal
==================

You can install qcfractal with ``conda``, with ``pip``, or by installing from source.

Conda
-----

You can update qcfractal using `conda <https://www.anaconda.com/download/>`_::

    conda install qcfractal -c conda-forge

This installs qcfractal and the NumPy dependancy.

The qcfractal package is maintained on the
`conda-forge channel <https://conda-forge.github.io/>`_.


Pip
---

To install qcfractal with ``pip`` there are a few options, depending on which
dependencies you would like to keep up to date:

*   ``pip install qcfractal``

Install from Source
-------------------

To install qcfractal from source, clone the repository from `github
<https://github.com/molssi/qcfractal>`_::

    git clone https://github.com/molssi/qcfractal.git
    cd qcfractal
    python setup.py install

or use ``pip`` for a local install::

    pip install -e .


Test
----

Test qcfractal with ``py.test``::

    cd qcfractal
    py.test
