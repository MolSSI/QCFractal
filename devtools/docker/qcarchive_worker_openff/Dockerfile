FROM continuumio/miniconda3
RUN conda install -c psi4 -c conda-forge "psi4 >=1.3.0" dftd3 gcp qcengine qcfractal rdkit geometric pytest
RUN groupadd -g 999 qcfractal && \
    useradd -m -r -u 999 -g qcfractal qcfractal
USER qcfractal
ENV PATH /opt/local/conda/envs/base/bin/:$PATH
ENTRYPOINT qcfractal-manager --config-file /etc/qcfractal-manager/manager.yaml
