# syntax=docker/dockerfile:1

FROM python:3.10-slim-buster

# psycopg2 needs some packages to build the C extension
RUN apt update
RUN apt install -y git libpq-dev gcc

# Config file is expected to be in /qcf_base
ENV QCF_BASE_FOLDER="/qcf_base"

# Can't own a database
ENV QCF_DB_OWN=False

WORKDIR /home/qcfuser
COPY ./ qca_src

RUN python -m pip install --upgrade pip
RUN pip install ./qca_src/qcportal ./qca_src/qcfractalcompute ./qca_src/qcfractal torsiondrive

RUN rm -Rf qca_src

COPY docker/entrypoint.sh /usr/local/bin

#COPY docker/GeoLite2-City.mmdb /qcf_base

ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
