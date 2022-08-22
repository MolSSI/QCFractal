# syntax=docker/dockerfile:1

FROM python:3.10-slim-buster

# psycopg2 needs some packages to build the C extension
RUN apt update
RUN apt install -y git libpq-dev gcc

# Regular user is all that's needed
RUN useradd -ms /bin/bash qcfuser
USER qcfuser

ENV PATH="/home/qcfuser/.local/bin:${PATH}"

# Config file is expected to be in /qcf_base
ENV QCF_CONFIG_PATH="/qcf_base/qcfractal_config.yaml"

WORKDIR /home/qcfuser
RUN git clone -b next https://github.com/MolSSI/QCFractal.git qcfractal

RUN python -m pip install --upgrade pip

# To be replaced with a proper pip install
RUN pip install --user ./qcfractal[geoip]

# Cleanup?
#RUN rm -Rf qcfractal

# Meant to be overridden
ENTRYPOINT [ "qcfractal-server" ]
