#!/usr/bin/env bash

# Check if MongoDB is running, filter grep itself
STOPMONGOD=false
if [[ $(ps -ax | grep mongo | grep -v grep) == "" ]]
then
    # Runs mongod on the default location, sends output to /dev/null and runs in background
    mongod --dbpath /data/db > /dev/null &
    sleep 5  # Give it a bit to boot up
    STOPMONGOD=true
fi

# Spin up a fireworks server in background
python server.py &
sleep 5  # Give it a bit to boot up

# Build, compute, and query database
python build_database.py
python compute_database.py
rlaunch -l fw_lpad.yaml rapidfire
python query_database.py

# Cleanup background processes we started
kill %%
if $STOPMONGOD;
then
    sleep 3  # Give some time to kill the fireworks server
    kill %%
fi
