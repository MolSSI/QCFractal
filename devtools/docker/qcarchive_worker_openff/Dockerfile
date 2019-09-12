FROM continuumio/miniconda3
RUN conda install -c psi4/label/dev -c conda-forge psi4 dftd3 mp2d qcengine qcfractal rdkit geometric 
RUN groupadd -g 999 qcfractal && \
    useradd -m -r -u 999 -g qcfractal qcfractal
USER qcfractal
ENV PATH /opt/local/conda/envs/base/bin/:$PATH
ENTRYPOINT qcfractal-manager --config-file /etc/qcfractal-manager/manager.yaml
