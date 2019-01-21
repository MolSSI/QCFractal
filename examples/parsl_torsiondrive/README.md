# TorsionDrive example using the Parsl backend

This example computes a torsiondrive using a custom Parsl configuration.  As
Parsl is not currently available to be configured from the command line this
template should be used for all Parsl compute managers. Full Parsl Config
documentation can be found
[here](https://parsl.readthedocs.io/en/stable/userguide/configuring.html#).

## Setup and Configure

Above the base QCFractal installation, this example requires the following
dependencies:
 - rdkit
 - geometric
 - torsiondrive

A complete environment can be created from the
[openff.yaml](../../devtools/conda-envs) conda environment.

## Example

### Step 1

It is recommended to run the MongoDB and QCFractal server in separate windows
so that you can see the logging information of interactions with the server.
In production, these are run in the background, and their log files can be
directed to a directory.

```bash
# Make MongoDB Directory and start server
# You can skip the mkdir command if you already have a folder somewhere
MONGOPATH=/tmp/fractalex
mkdir -p $MONGOPATH
mongod --dbpath $MONGOPATH
```

```bash
qcfractal-server qca_parsl_testing
```

### Step 2

Add a torsion computation to the server.

```bash
python compute_torsion.py
```

This is a single service run, note that since there is no compute attached to the server
nothing happens and you will be unable to query the result.

### Step 3
A Parsl manager can be spun up via the following command:
```bash
python parsl_manager.py
```

As Parsl is only available to be configured in code this script needs to be
used for all Parsl manager runs. This is currently setup for a local process
compute engine, but can be configured to run on hundreds of thousands of cores.

Let this process run until the manager receives no new services from the server.

### Step 4
Query data:
```bash
python query_database.py
``` 

### Cleanup
You can now stop the Server and MongoDB processes.

If you wish to remove data from the server you can run this script:
```bash
python reset_server.py
```

*Warning!* This can be a very dangerous operation as it will remove all stored
data.  This operation should likely not be used in production and provides
testing functionality only.
