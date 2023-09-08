# QCArchive

<p align="center">
    <picture>
      <source media="(prefers-color-scheme: light)" srcset="https://molssi.github.io/QCFractal/_static/molssi_main_logo.png">
      <source media="(prefers-color-scheme: dark)" srcset="https://molssi.github.io/QCFractal/_static/molssi_main_logo_inverted_white.png">
      <img alt="MolSSI Logo" src="https://molssi.github.io/QCFractal/_static/molssi_main_logo.png" height="100px">
    </picture>
    <picture>
      <source media="(prefers-color-scheme: light)" srcset="https://molssi.github.io/QCFractal/_static/qcarchive_logo.svg">
      <source media="(prefers-color-scheme: dark)" srcset="https://molssi.github.io/QCFractal/_static/qcarchive_logo_inverted.svg">
      <img alt="QCArchive Logo" src="https://molssi.github.io/QCFractal/_static/qcarchive_logo.svg" height="100px">
    </picture>
</p>


A platform for compute, managing, compiling, and sharing large amounts of quantum chemistry data

## Introduction

QCArchive is a platform that makes running large numbers of quantum chemistry calculations in a
robust and scalable manner accessible to computational chemists.
QCArchive is designed to handle thousands to millions of computations,
storing them in a database for later sharing, retrieval and analysis, or export.

## Documentation

Full documentation available [here](https://molssi.github.io/QCFractal)

## Installing from the git repo

To install these packages with pip directly from this git repository,

```shell
pip install ./qcportal ./qcfractal ./qcfractalcompute ./qcarchivetesting
```

or, for a developer (editable) install,

```shell
pip install -e ./qcportal -e ./qcfractal -e ./qcfractalcompute -e ./qcarchivetesting
```

## About this repository

This repository follows a [monorepo](https://en.wikipedia.org/wiki/Monorepo) layout.
That is, this single repository contains several different python packages, each with its
own setup information (`pyproject.toml`).

 * `qcfractal` - The main QCFractal server (database and web API)
 * `qcportal` - Python client for interacting with the server
 * `qcfractalcompute` - Workers that are deployed to run computations
 * `qcarchivetesting` - Helpers and pytest harnesses for testing QCArchive components

The reason for this is that at this stage, these components are very dependent on each other, and
change one often requires changing others. This layout allows for that, while also being able
to create/distribute separate python packages (that is, `qcportal` can be packaged separately and uploaded to
PyPI or conda-forge).

##  License

BSD-3C. See the [License File](LICENSE) for more information.
