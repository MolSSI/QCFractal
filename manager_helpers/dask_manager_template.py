"""
Dask Distributed Manager Helper

Conditions:
- Dask Distributed and Dask Job Queue (dask_jobqueue in Conda/pip)
- Manager running on the head node
- SLURM manager

For additional information about the Dask Job Queue, please visit this site:
https://jobqueue.dask.org/en/latest/
"""

# Fractal Settings
# Location of the Fractal Server you are connecting to
FRACTAL_ADDRESS = "localhost:7777"

# How many cores per node you want your jobs to have access to
CORES_PER_NODE = 0
# How much memory per node (in GB) you want your jobs to have access to
MEMORY_PER_NODE = 0
# How many tasks per node you want to execute on
MAX_TASKS_PER_NODE = 0
# Maximum number of nodes to try and consume
MAX_NODES = 10
# Whether or not to claim nodes for exclusive use. We recommend you do, but that's up to you
NODE_EXCLUSIVITY = True

# Generic Cluster Settings
# Additional commands to send to the command line (often used as "#SBATCH ..." or '#PBS' headers.)
# This is a per-node type setting, not task.
# Don't set memory or cpu or wall clock through this
SCHEDULER_OPTS = []
# Additional commands to start each task with. E.g. Activating a conda environment
TASK_STARTUP_COMMANDS = ''

# SLURM Specific Settings
# Name of the SLURM partition to draw from
SLURM_PARTITION = ''


###################

# QCFractal import
import qcfractal
import qcfractal.interface as portal

# Make sure logging is setup correctly
import tornado.log
tornado.log.enable_pretty_logging()

from dask_jobqueue import SLURMCluster
from dask.distributed import Client


# Quick sanity checks

if CORES_PER_NODE < 1 or not isinstance(CORES_PER_NODE, int):
    raise ValueError("CORES_PER_NODE must be an integer of at least 1")
if MAX_TASKS_PER_NODE < 1 or not isinstance(MAX_TASKS_PER_NODE, int):
    raise ValueError("MAX_TASKS_PER_NODE must be an integer of at least 1")
if MAX_NODES < 1 or not isinstance(MAX_NODES, int):
    raise ValueError("MAX_NODES must be an integer of at least 1")
if MEMORY_PER_NODE <= 0:
    raise ValueError("MEMORY_PER_NODE must be a number > 0")
if NODE_EXCLUSIVITY and "--exclusive" not in SCHEDULER_OPTS:
    SCHEDULER_OPTS.append("--exclusive")

cluster = SLURMCluster(
    name='QCFractal Dask Compute Executor',
    cores=CORES_PER_NODE,
    memory=str(MEMORY_PER_NODE) + "GB",
    queue=SLURM_PARTITION,
    processes=MAX_TASKS_PER_NODE,  # This subdivides the cores by the number of processes we expect to run
    walltime="00:10:00",

    # Additional queue submission flags to set
    job_extra=SCHEDULER_OPTS,
    # Not sure of the validity of this, but it seems to be the only terminal-invoking way
    # so python envs may be setup from there
    # Commands to execute before the Dask
    env_extra=TASK_STARTUP_COMMANDS
)

# Setup up adaption
# Workers are distributed down to the cores through the sub-divided processes
# Optimization may be needed
cluster.adapt(minimum=0, maximum=MAX_NODES)

dask_client = Client(cluster)


# Build a interface to the server
client = portal.FractalClient(FRACTAL_ADDRESS, verify=False)

# Build a manager
manager = qcfractal.queue.QueueManager(client, dask_client, update_frequency=0.5,
                                       cores_per_task=CORES_PER_NODE // MAX_TASKS_PER_NODE,
                                       memory_per_task=MEMORY_PER_NODE // MAX_TASKS_PER_NODE)

# Important for a calm shutdown
from qcfractal.cli.cli_utils import install_signal_handlers
install_signal_handlers(manager.loop, manager.stop)

# Start the loop
manager.start()
