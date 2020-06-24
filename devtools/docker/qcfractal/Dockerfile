FROM continuumio/miniconda3
SHELL ["/bin/bash", "-c"]
RUN conda install anaconda-client
RUN conda env create -n qcfractal qcarchive/qcfractal
RUN groupadd -g 999 qcfractal && \
    useradd -m -r -u 999 -g qcfractal qcfractal
USER qcfractal
ENV PATH /opt/conda/envs/qcfractal/bin/:$PATH
RUN echo "source activate qcfractal" > ~/.bashrc