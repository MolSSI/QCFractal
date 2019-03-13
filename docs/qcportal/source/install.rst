Install QCFractal
=================

You can install qcportal with ``conda``, with ``pip``, or by installing from source.

Conda
-----

You can update qcportal using `conda <https://www.anaconda.com/download/>`_::

    conda install qcportal -c conda-forge

This installs qcportal and the NumPy dependancy.

The qcportal package is maintained on the
`conda-forge channel <https://conda-forge.github.io/>`_.


Pip
---

To install qcportal with ``pip`` there are a few options, depending on which
dependencies you would like to keep up to date:

*   ``pip install qcportal``

Developer Install
-----------------

QCPortal is part of the QCFractal project and is the ``qcfractal.interface``
folder. To install QCFractal from source, clone the repository from `github
<https://github.com/molssi/qcfractal>`_::

    git clone https://github.com/molssi/qcfractal.git
    cd qcfractal
    pip install -e .

