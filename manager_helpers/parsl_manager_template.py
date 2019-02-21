"""
Parsl Manager Helper

Conditions:
- Parsl
- Manager running on the head node
- SLURM manager

For additional information about the Parsl config, please visit this site:
https://parsl.readthedocs.io/en/latest/userguide/configuring.html
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
# Additional commands to send to the "#SBATCH ..." headers. This is a per-node type setting, not task,
# Don't set memory or cpu or wall clock through this
SCHEDULER_OPTS = ''
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

# Setup a custom configuration
from parsl.channels import SSHChannel
from parsl.providers import SlurmProvider

from parsl.config import Config
from parsl.executors.ipp import IPyParallelExecutor

# Quick sanity checks

if CORES_PER_NODE < 1 or not isinstance(CORES_PER_NODE, int):
    raise ValueError("CORES_PER_NODE must be an integer of at least 1")
if MAX_TASKS_PER_NODE < 1 or not isinstance(MAX_TASKS_PER_NODE, int):
    raise ValueError("MAX_TASKS_PER_NODE must be an integer of at least 1")
if MAX_NODES < 1 or not isinstance(MAX_NODES, int):
    raise ValueError("MAX_NODES must be an integer of at least 1")
if MEMORY_PER_NODE <= 0:
    raise ValueError("MEMORY_PER_NODE must be a number > 0")

config = Config(
    executors=[
        IPyParallelExecutor(
            label='QCFractal Parsl Compute Executor',
            provider=SlurmProvider(
                SLURM_PARTITION,
                scheduler_options=SCHEDULER_OPTS,
                worker_init=TASK_STARTUP_COMMANDS,
                walltime="00:10:00",
                exclusive=NODE_EXCLUSIVITY,
                init_blocks=1,
                max_blocks=MAX_NODES,
                nodes_per_block=1,        # Keep one node per block, its just easier this way
            ),
            workers_per_node=MAX_TASKS_PER_NODE,
        )

    ],
)


# Build a interface to the server
client = portal.FractalClient(FRACTAL_ADDRESS, verify=False)

# Build a manager
manager = qcfractal.queue.QueueManager(client, config, update_frequency=0.5,
                                       cores_per_task=CORES_PER_NODE // MAX_TASKS_PER_NODE,
                                       memory_per_task=MEMORY_PER_NODE // MAX_TASKS_PER_NODE)

# Important for a calm shutdown
from qcfractal.cli.cli_utils import install_signal_handlers
install_signal_handlers(manager.loop, manager.stop)

# Start the loop
manager.start()
