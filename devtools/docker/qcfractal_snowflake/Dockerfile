FROM continuumio/miniconda3
SHELL ["/bin/bash", "-c"]
RUN conda install anaconda-client
RUN conda env create -n qcfractal qcarchive/qcfractal-snowflake
RUN /opt/conda/envs/qcfractal/bin/jupyter-nbextension enable nglview --py --sys-prefix
RUN groupadd -g 999 qcfractal && \
    useradd -m -r -u 999 -g qcfractal qcfractal
USER qcfractal
ENV PATH /opt/conda/envs/qcfractal/bin/:$PATH
RUN echo "source activate qcfractal" > ~/.bashrc
EXPOSE 8888
COPY quickstart.ipynb /home/qcfractal/demo.ipynb
ENTRYPOINT ["/opt/conda/envs/qcfractal/bin/jupyter-notebook", "--ip=0.0.0.0", "--no-browser", "--notebook-dir=/home/qcfractal/"]
