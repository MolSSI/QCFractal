# syntax=docker/dockerfile:1

FROM python:3.10-slim-buster

# psycopg2 needs some packages to build the C extension
RUN apt-get update
RUN apt-get install -y git libpq-dev gcc

RUN useradd -ms /bin/bash qcarchive
USER qcarchive
WORKDIR /home/qcarchive

ENV PATH="/home/qcarchive/.local/bin:${PATH}"
RUN python -m pip install --user --upgrade pip
RUN pip --version
RUN which pip
RUN which python

# Copy the source, install it, and remove the source
COPY --chown=qcarchive:qcarchive ./ qca_src
RUN pip install --user ./qca_src/qcportal ./qca_src/qcfractalcompute ./qca_src/qcfractal[services]
RUN rm -Rf qca_src

COPY docker/entrypoint.sh /usr/local/bin
ENTRYPOINT [ "/usr/local/bin/entrypoint.sh" ]
