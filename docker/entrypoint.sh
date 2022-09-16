#!/bin/bash

set -e

# Only do this if we aren't initializing the config or the db
# Safe to do even if the db exists already
if [[ "$@" != *"init-config"*  && "$@" != *"init-db"* && "$@" != *"--help"* ]]
then
    qcfractal-server init-db
fi

# Run with the rest of the arguments
# You can set QCF_DOCKER_COMMAND in an environment variable to specify the command,
# or just pass the arguments to "docker run"
qcfractal-server $@ ${QCF_DOCKER_COMMAND}
