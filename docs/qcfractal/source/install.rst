Install QCFractal
=================

You can install qcfractal with ``conda``, with ``pip``, or by installing from source.

Conda
-----

You can install or update qcfractal using `conda <https://www.anaconda.com/download/>`_::

    conda install qcfractal -c conda-forge

The above command installs qcfractal and its required dependencies, but *not* any of the quantum
chemistry codes nor the software to run :term:`Queue Managers <Manager>`. This is done to avoid requiring *all* software
which *can* interface with Fractal, and instead requires the user to obtain the software they individually *require*.

The qcfractal package is maintained on the
`conda-forge channel <https://conda-forge.github.io/>`_.


Conda Pre-Created Environments
++++++++++++++++++++++++++++++

Fractal can also be installed through pre-configured environments you can pull through our Conda Channel::

    conda env create qcarchive/{environment name}
    conda activate {environment name}

The environments are created from the YAML files hosted on the Anaconda Cloud, which then need to be activated
to use. You can find all of the environments `here <https://anaconda.org/QCArchive/environments>`_.

If you want to use a different name than the environment file, you can add a ``-n {custom name}`` flag to the
``conda env`` command.

The environments must be installed as new environments and cannot be installed into existing ones.

Pip
---

To install qcfractal with ``pip`` there are a few options, depending on which
dependencies you would like to keep up to date:

*   ``pip install qcfractal``

.. XXX: only one option is listed here. 

Install from Source
-------------------

To install qcfractal from source, clone the repository from `github
<https://github.com/molssi/qcfractal>`_::

    git clone https://github.com/molssi/qcfractal.git
    cd qcfractal
    python setup.py install

or use ``pip`` for a local install::

    pip install -e .

It is recommended to setup a testing environment using ``conda``. This can be accomplished by::

    cd qcfractal
    python devtools/scripts/conda_env.py -n=qcf_test -p=3.7 devtools/conda-envs/openff.yaml
    conda activate qcarchive
    pip install -e .

This installs all the dependencies to setup a production background in a new conda environment,
activate the environment, and then install Fractal into development mode.

Test
----

Test qcfractal with ``pytest``::

    cd qcfractal
    pytest

