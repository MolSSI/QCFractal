Install QCPortal
=================

QCPortal can be installed using ``conda`` or ``pip``.

Conda
-----

The following command installs QCPortal and its dependencies 
using `conda <https://www.anaconda.com/download/>`_:

.. code-block:: console

   >>> conda install qcportal -c conda-forge

The QCPortal package is maintained on the 
`conda-forge channel <https://conda-forge.github.io/>`_.

Pip
---

QCPortal can also be installed using ``pip``:

.. code-block:: console

   >>> pip install qcportal


Testing the Installation
------------------------

The installation process for QCPortal can be verified
using ``pytest`` through running the tests.
The ``pytest`` package can be installed using ``conda``:

.. code-block:: console

   >>> conda install pytest -c conda-forge

or ``pip``:

.. code-block:: console

   >>> pip install pytest

After installing ``pytest``, the following command 
collects the tests and runs them individually in order to 
verify the installation:

.. code-block::

   >>> pytest --pyargs qcportal


Developing from Source
----------------------

The QCPortal is a part of the QCFractal package and resides in the ``qcfractal.interface`` folder.
Developers can make contributions to QCPortal by accessing the source code
`here <https://github.com/MolSSI/QCFractal/tree/master/qcfractal/interface>`_.
