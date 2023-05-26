# QCArchive

<p align="center">
    <picture style>
      <source media="(prefers-color-scheme: dark)" srcset="https://molssi.github.io/QCFractal/_static/molssi_main_logo.png">
      <source media="(prefers-color-scheme: light)" srcset="https://molssi.github.io/QCFractal/_static/molssi_main_logo_inverted_white.png">
      <img alt="MolSSI Logo" src="https://molssi.github.io/QCFractal/_static/molssi_main_logo.png" height=100">
    </picture>
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://molssi.github.io/QCFractal/_static/qcarchive_logo.svg">
      <source media="(prefers-color-scheme: light)" srcset="https://molssi.github.io/QCFractal/_static/qcarchive_logo_inverted.svg">
      <img alt="MolSSI Logo" src="https://molssi.github.io/QCFractal/_static/qcarchive_logo.svg" height=100">
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

## Demonstration server

A publicly-available demonstration server is available to try out with zero installation!
The server is accessed via Jupyter notebooks hosted on BinderHub.
[Click here](https://qcademo.molssi.org) to access the server.

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
