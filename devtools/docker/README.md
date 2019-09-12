# QCFractal Dockerfiles

QCFractal Dockerfiles are provided herein and correspond to images provided on [Docker Hub](https://cloud.docker.com/u/molssi/repository/list).

## qcfractal

This Dockerfile contains QCFractal.

## qcarchive_worker_openff

This Dockerfile builds a container intended to be used as a compute worker.
It contains QCFractal as well as tools used by the [OpenFF](https://openforcefield.org/) workflow:

* [Psi4](http://www.psicode.org), [dftd3](https://github.com/loriab/dftd3), and [MP2D](https://github.com/Chandemonium/MP2D>)
* [RDKit](https://www.rdkit.org)
* [geomeTRIC](https://github.com/leeping/geomeTRIC)

Its entrypoint launches a compute manager based on the configuration file, which must be provided at `/etc/qcfractal-manager/manager.yaml`.