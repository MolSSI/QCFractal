# Fireworks Server Example

## Setup and Configure

This example assumes you have a install configure to the setup in the
[fireworks.yaml or openff.yaml conda env](../../devtools/conda-envs).

You effectively need MongoDB and Fireworks installed

You will also need to configure your MongoDB to write to location 
you have write access to. The example sets one but you can change 
it to where ever you like.


## Example

### Step 1

Run the MongoDB and QCFractal server in the background or a new window.

We recommend running these commands in separate windows so that you can
see the logging information of interactions with the server.

```bash
# Make MongoDB Directory and start server
# You can skip the mkdir command if you already have a folder somewhere
MONGOPATH=/tmp/fractalex
mkdir -p $MONGOPATH
mongod --dbpath $MONGOPATH
```

The QCFractal server can be started with a fireworks-queue manager in the
database `qca_fw_testing`. This will automatically create a fireworks queue
with the name `qca_fw_testing_fireworks_queue`.

```bash
qcfractal-server qca_fw_testing --fireworks-manager
```

### Step 2

Add a new Database of several intermolecular reactions. 

This step and following ones should be run in the same window and/or 
as foreground processes separate from the MongoDB and Fireworks servers.

```bash
python build_database.py
```

### Step 3
Add new computation to the queue
```bash
python compute_database.py
```

### Step 4
Execute jobs in the queue:
```bash
rlaunch -l fw_lpad.yaml rapidfire
```

### Step 5
Query data
```bash
python query_database.py
``` 

### Cleanup
You can now stop the Fireworks and MongoDB processes

If you wish to remove data from the server you can run the script
```bash
python reset_server.py
```

*Warning!* This can be a very dangerous operation as it will remove all stored
data.  This operation should likely not be used in production and provides
testing functionality only.
