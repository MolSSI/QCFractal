"""
A canonical Parsl manager example
"""

# QCFractal import
import qcfractal
import qcfractal.interface as portal

# Make sure logging is setup correctly
import tornado.log
tornado.log.enable_pretty_logging()

# Import a process config
from parsl.configs.local_ipp import config

# Setup a custom configuration
# from parsl.channels import SSHChannel
# from parsl.providers import SlurmProvider
# 
# from parsl.config import Config
# from parsl.executors.ipp import IPyParallelExecutor
# from parsl.executors.ipp_controller import Controller
# config = Config(
#     executors=[
#         IPyParallelExecutor(
#             label='canonical_slurm',
#             provider=SlurmProvider(
#                 'debug',                  # Channel
#                 scheduler_options='',     # Input your scheduler_options if needed
#                 worker_init='conda activate qcf',  # Activate the conda environment
#                 walltime="00:10:00",
#                 init_blocks=1,
#                 max_blocks=1,
#                 nodes_per_block=1,        # Keep one node per block
#             ),
#         )
# 
#     ],
# )


# Build a interface to the server 
client = portal.FractalClient("localhost:7777", verify=False)

# Build a manager
manager = qcfractal.queue.QueueManager(client, config, update_frequency=0.5)

# Important for a calm shutdown
from qcfractal.cli.cli_utils import install_signal_handlers
install_signal_handlers(manager.loop, manager.stop)

# Start the loop
manager.start()
