Fireworks Server Example
========================

# Steps

## Step 1
Run the server in the background or a new window.
```
python server.py
```

We recommend running this command in a window so that you can
see the logging information of interactions with the server.


## Step 2
Add a new Database of several intermolecular reactions. 
```
python build_database.py
```

## Step 3
Add new computation to the queue
```
python compute_database.py
```

## Step 4
Execute jobs in the queue:
```
rlaunch -l fw_lpad.yaml rapidfire
```

## Step 5
Query data
```
python query_database.py
``` 
