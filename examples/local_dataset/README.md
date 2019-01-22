# Dataset example using a local compute backend

This example computes several interaction energies using the Dataset Collection
using the Python ProcessPoolManager backend.

## Setup and Configure

Above the base QCFractal installation, this example requires the following
dependencies:
 - Psi4 (`conda install psi4 -c psi4`)

It is recommend to create a development environment from either the
[base.yaml or openff.yaml conda envs](../../devtools/conda-envs).

## Example

### Step 1

It is recommended to run the MongoDB and QCFractal server in separate windows
so that you can see the logging information of interactions with the server.
In production, these are run in the background and their log files can be
directed to a directory.

```bash
# Make MongoDB Directory and start server
# You can skip the mkdir command if you already have a folder somewhere
MONGOPATH=/tmp/fractalex
mkdir -p $MONGOPATH
mongod --dbpath $MONGOPATH
```

The QCFractal server can be started with a `fireworks queue-manager` in the
database `qca_fw_testing`. This will automatically create a fireworks queue
with the name `qca_fw_testing_fireworks_queue`.

```bash
qcfractal-server qca_local_testing --local-manager
```

### Step 2

A new Database Collection can be added to the server with several systems to be computed.
Add a new Database of several intermolecular reactions. 

This step and following ones should be run in the same window and/or 
as foreground processes separate from the MongoDB and FractalServer windows.

```bash
python build_database.py
```

### Step 3
Add new computation to the queue:
```bash
python compute_database.py
```

At this point you should notice the FractalServer logging information
detail new and completed tasks.

### Step 5
The results can be queried once the tasks have completed.
```bash
python query_database.py
``` 


### Cleanup
You can now stop the FractalServer and MongoDB processes

If you wish to remove data from the server you can run the script
```bash
python reset_server.py
```

*Warning!* This can be a very dangerous operation as it will remove all stored
data.  This operation should likely not be used in production and provides
testing functionality only.
