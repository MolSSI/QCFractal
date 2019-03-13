"""
A command line interface to generate qcfractal manager scripts
"""

import argparse
from textwrap import dedent, indent

__all__ = ["main"]

# Using backslash on opening triple quotes ('''\) to indicate no new line after from that line return in final

base_script = '''\
"""
{MANAGER_NAME_SHORT} Manager Helper

Conditions:
- {MANAGER_NAME_LONG}
- Manager running on the head node
- {CLUSTER_QUEUE_NAME} manager

For additional information about the {MANAGER_URL_TITLE}, please visit this site:
{MANAGER_URL}
"""

# Fractal Settings
# Location of the Fractal Server you are connecting to
FRACTAL_URI = "localhost:7777"  # QCArchive is at: api.qcarchive.molssi.org:443
# Authentication with the Fractal Server
USERNAME = None
PASSWORD = None
VERIFY_SSL = False


# Queue Manager Settings
# Set whether or not we are just testing the Queue Manger (no Fractal Client needed)
TEST_RUN = {TEST_RUN}
# Tell the manager to only pull jobs with this tag
QUEUE_TAG = None



# How many cores per node you want your jobs to have access to
CORES_PER_NODE = 1
# How much memory per node (in GB) you want your jobs to have access to
MEMORY_PER_NODE = 1
# How many tasks per node you want to execute on
MAX_TASKS_PER_NODE = 1
# Maximum number of nodes to try and consume
MAX_NODES = 1
# Whether or not to claim nodes for exclusive use. We recommend you do, but that's up to you
NODE_EXCLUSIVITY = True

# Generic Cluster Settings
# ========================
# Additional commands to send to the command line (often used as "#SBATCH ..." or '#PBS' headers.)
# This is a per-node type setting, not task. Don't set memory or cpu or wall clock through this
# -- Note ---
# Different Managers interpret this slightly differently, but that should not be your concern, just treat
# each item as though it were a CLI entry and the manager block will interpret
# ------------
SCHEDULER_OPTS = []

# Additional commands to start each task with. E.g. Activating a conda environment
# Put each command as its own item in strings
TASK_STARTUP_COMMANDS = []

{CLUSTER_QUEUE_FLAGS}

###################

# QCFractal import
import qcfractal
import qcfractal.interface as portal

# Make sure logging is setup correctly
import tornado.log
tornado.log.enable_pretty_logging()

{IMPORTS}

# Quick sanity checks

if CORES_PER_NODE < 1 or not isinstance(CORES_PER_NODE, int):
    raise ValueError("CORES_PER_NODE must be an integer of at least 1")
if MAX_TASKS_PER_NODE < 1 or not isinstance(MAX_TASKS_PER_NODE, int):
    raise ValueError("MAX_TASKS_PER_NODE must be an integer of at least 1")
if MAX_NODES < 1 or not isinstance(MAX_NODES, int):
    raise ValueError("MAX_NODES must be an integer of at least 1")
if MEMORY_PER_NODE <= 0:
    raise ValueError("MEMORY_PER_NODE must be a number > 0")
{SANITY_CHECKS}

{MANAGER_CLIENT_BUILDER}

# Build a interface to the server
# If testing, there is no need to point to a Fractal Client and None is passed in
# In production, the client is needed
if TEST_RUN:
    fractal_client = None
else:
    fractal_client = portal.FractalClient(FRACTAL_URI, 
                                          username=USERNAME,
                                          password=PASSWORD,
                                          verify=VERIFY_SSL)



# Build a manager
manager = qcfractal.queue.QueueManager(fractal_client, {MANAGER_CLIENT}, update_frequency=0.5,
                                       cores_per_task=CORES_PER_NODE // MAX_TASKS_PER_NODE,
                                       memory_per_task=MEMORY_PER_NODE // MAX_TASKS_PER_NODE,
                                       queue_tag=QUEUE_TAG)

# Important for a calm shutdown
from qcfractal.cli.cli_utils import install_signal_handlers
install_signal_handlers(manager.loop, manager.stop)

# Start or test the loop. Swap with the .test() and .start() method respectively
if TEST_RUN:
    manager.test()
else:
    manager.start()
'''

torque_helper = {
    "CLUSTER_QUEUE_NAME": "PBS/Torque",

    "CLUSTER_QUEUE_FLAGS": dedent("""\
        # TORQUE Specific Settings
        # Name of the Torque Queue to request resources from, set `None` if you don't know (may be cluster specific)
        # Equivalent to the `#PBS -q` option
        TORQUE_QUEUE = None
        # Name of the Torque account/project to charge, set `None` if using default scheduler (may be cluster specific)
        # Equivalent to the `#PBS -A` option
        TORQUE_ACCOUNT = None
        """)
}

slurm_helper = {
    "CLUSTER_QUEUE_NAME": "SLURM",

    "CLUSTER_QUEUE_FLAGS": dedent("""\
        # SLURM Specific Settings
        # Name of the SLURM partition to draw from
        SLURM_PARTITION = ''
        """)
}

lsf_helper = {
    "CLUSTER_QUEUE_NAME": "LSF",

    "CLUSTER_QUEUE_FLAGS": dedent("""\
        # LSF Specific Settings
        # Name of the LSF Queue to request resources from, set `None` if you don't know (may be cluster specific)
        # Equivalent to the `#BSUB -q` option
        LSF_QUEUE = None
        # Name of the LSF project to charge, set `None` if using default scheduler (may be cluster specific)
        # Equivalent to the `#BSUB -P` option
        LSF_PROJECT = None
        """)
}

scheduler_collections = {
    "slurm": slurm_helper,
    "torque": torque_helper,
    "lsf": lsf_helper
}

test_helper = {
    True: {  # Tests are set, comment stuff out
        "TEST_RUN": "True",
    },
    False: {  # Production, connect to Fractal
        "TEST_RUN": "False",
    }
}


def dask_templates():

    base_dict = {
        "MANAGER_NAME_SHORT": "Dask Distributed",
        "MANAGER_NAME_LONG": "Dask Distributed and Dask Job Queue (dask_jobqueue in Conda/pip)",
        "MANAGER_URL_TITLE": "Dask Job Queue",
        "MANAGER_URL": "https://jobqueue.dask.org/en/latest/",
        "MANAGER_CLIENT": "dask_client",
        "SANITY_CHECKS": "",
        "IMPORTS": """from dask.distributed import Client""",
        "MANAGER_CLIENT_BUILDER": ""
    }

    code_skeletal = dedent("""\
        {BUILDER}

        # Setup up adaption
        # Workers are distributed down to the cores through the sub-divided processes
        # Optimization may be needed
        cluster.adapt(minimum=0, maximum=MAX_NODES)

        # Integrate cluster with client
        dask_client = Client(cluster)

        """)

    # SLURM

    slurm_imports = base_dict["IMPORTS"] + "\n" + """from dask_jobqueue import SLURMCluster"""

    slurm_sanity_checks = dedent("""\
        if NODE_EXCLUSIVITY and "--exclusive" not in SCHEDULER_OPTS:
            SCHEDULER_OPTS.append("--exclusive")
        """)

    slurm_builder = dedent("""\
        cluster = SLURMCluster(
            name='QCFractal_Dask_Compute_Executor',
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
            env_extra=TASK_STARTUP_COMMANDS,
            # Uncomment and set this if your cluster uses non-standard ethernet port names
            # for communication between the head node and your compute nodes
            # interface="eth0"
            extra=['--resources process=1'],
        )
        """)

    # PBS/Torque

    torque_imports = base_dict["IMPORTS"] + "\n" + """from dask_jobqueue import PBSCluster"""

    torque_builder = dedent("""\
        cluster = PBSCluster(
            name='QCFractal_Dask_Compute_Executor',
            cores=CORES_PER_NODE,
            memory=str(MEMORY_PER_NODE) + "GB",
            queue=TORQUE_QUEUE,
            project=TORQUE_ACCOUNT,
            processes=MAX_TASKS_PER_NODE,  # This subdivides the cores by the number of processes we expect to run
            walltime="00:10:00",

            # Additional queue submission flags to set
            job_extra=SCHEDULER_OPTS,
            # Not sure of the validity of this, but it seems to be the only terminal-invoking way
            # so python envs may be setup from there
            # Commands to execute before the Dask
            env_extra=TASK_STARTUP_COMMANDS,
            # Uncomment and set this if your cluster uses non-standard ethernet port names
            # for communication between the head node and your compute nodes
            # interface="eth0"
            extra=['--resources process=1'],
        )
        """)

    # LSF

    lsf_imports = base_dict["IMPORTS"] + "\n" + """from dask_jobqueue import LSFCluster"""

    lsf_builder = dedent("""\
        cluster = LSFCluster(
            name='QCFractal_Dask_Compute_Executor',
            cores=CORES_PER_NODE,
            memory=str(MEMORY_PER_NODE) + "GB",
            queue=LSF_QUEUE,
            project=LSF_PROJECT,
            processes=MAX_TASKS_PER_NODE,  # This subdivides the cores by the number of processes we expect to run
            walltime="00:10:00",

            # Additional queue submission flags to set
            job_extra=SCHEDULER_OPTS,
            # Not sure of the validity of this, but it seems to be the only terminal-invoking way
            # so python envs may be setup from there
            # Commands to execute before the Dask
            env_extra=TASK_STARTUP_COMMANDS,
            # Uncomment and set this if your cluster uses non-standard ethernet port names
            # for communication between the head node and your compute nodes
            # interface="eth0"
            extra=['--resources process=1'],
        )
        """)

    # Final

    dask_dict = {
        "slurm": {**base_dict,
                  **{"SANITY_CHECKS": slurm_sanity_checks,
                     "IMPORTS": slurm_imports,
                     "MANAGER_CLIENT_BUILDER": code_skeletal.format(BUILDER=slurm_builder),
                     }
                  },
        "torque": {**base_dict,
                   **{"IMPORTS": torque_imports,
                      "MANAGER_CLIENT_BUILDER": code_skeletal.format(BUILDER=torque_builder)}
                   },
        "lsf": {**base_dict,
                **{"IMPORTS": lsf_imports,
                   "MANAGER_CLIENT_BUILDER": code_skeletal.format(BUILDER=lsf_builder)}
                }
    }

    return dask_dict, base_dict


def parsl_templates():

    base_dict = {
        "MANAGER_NAME_SHORT": "Parsl",
        "MANAGER_NAME_LONG": "Parsl Parallel Scripting Library",
        "MANAGER_URL_TITLE": "Parsl",
        "MANAGER_URL": "https://parsl.readthedocs.io/en/latest/index.html",
        "MANAGER_CLIENT": "parsl_config",
        "SANITY_CHECKS": "",
        "IMPORTS": dedent("""\
            from parsl.config import Config
            from parsl.executors import HighThroughputExecutor"""
                          ),

        "MANAGER_CLIENT_BUILDER": ""
    }

    code_skeletal = dedent("""\
        parsl_config = Config(
            executors=[
                HighThroughputExecutor(
                    label='QCFractal_Compute_Executor',
                    provider={PROVIDER}(
                        {PROVIDER_OPTS}
                        scheduler_options={COMBINED_SCHEDULER_OPTS}
                        worker_init='\\n'.join(TASK_STARTUP_COMMANDS),
                        walltime="00:10:00",
                        init_blocks=1,
                        max_blocks=MAX_NODES,
                        nodes_per_block=1,        # Keep one node per block, its just easier this way
                    ),
                    # workers_per_node=MAX_TASKS_PER_NODE,
                    cores_per_worker=CORES_PER_NODE // MAX_TASKS_PER_NODE,
                    max_workers = MAX_NODES*MAX_TASKS_PER_NODE
                )

            ],
        )"""
                           )
    whitespace = ''
    for line in code_skeletal.splitlines():
        if "PROVIDER_OPTS" in line:
            whitespace = line[:len(line) - len(line.lstrip())]
            break

    # break this out down here as the "\n" causes issues
    # Also have to \\n otherwise dedent() detects it wrong
    scheduler_opts = "'{SCHEDULER_HEADER} ' + '\\n{SCHEDULER_HEADER} '.join(SCHEDULER_OPTS) + '\\n',"

    # SLURM

    slurm_imports = base_dict["IMPORTS"] + "\n" + """from parsl.providers import SlurmProvider"""

    slurm_builder = {"PROVIDER": "SlurmProvider",
                     "PROVIDER_OPTS":
                         indent(dedent("""\
                         SLURM_PARTITION,
                         exclusive=NODE_EXCLUSIVITY,"""),
                                whitespace,
                                # Don't indent first line, use whatever logic
                                predicate=lambda feed: "=" in feed),
                     "COMBINED_SCHEDULER_OPTS": scheduler_opts.format(SCHEDULER_HEADER="#SBATCH")}

    # PBS/Torque

    torque_imports = base_dict["IMPORTS"] + "\n" + """from parsl.providers import TorqueProvider"""

    torque_builder = {"PROVIDER": "TorqueProvider",
                      "PROVIDER_OPTS":
                          indent(dedent("""\
                          account=TORQUE_ACCOUNT,
                          queue=TORQUE_QUEUE,"""),
                                 whitespace,
                                 # Don't indent first line, use whatever logic
                                 predicate=lambda feed: "account" not in feed),
                      "COMBINED_SCHEDULER_OPTS": scheduler_opts.format(SCHEDULER_HEADER="#PBS")}
    # Final

    parsl_dict = {
        "slurm": {**base_dict,
                  **{"IMPORTS": slurm_imports,
                     "MANAGER_CLIENT_BUILDER": code_skeletal.format(**slurm_builder),
                     }
                  },
        "torque": {**base_dict,
                   **{"IMPORTS": torque_imports,
                      "MANAGER_CLIENT_BUILDER": code_skeletal.format(**torque_builder)}
                   },
    }

    return parsl_dict, base_dict


def parse_args():
    parser = argparse.ArgumentParser(description='A Generator for QCFractal QueueManager scripts.')
    parser.add_argument("adapter",
                        type=str,
                        help="The adapter and distribution system to build the template from")
    parser.add_argument('scheduler',
                        type=str,
                        help="The Cluster scheduler on the resource you plan to execute on (e.g. slurm, pbs/torque\n"
                             "This is capitalization agnostic, and 'torque'<->'pbs' are interchangeable")
    parser.add_argument('--test',
                        dest="test",
                        action="store_true",
                        help="Set whether or not to generate a test script or a production script")
    parser.add_argument('-o', '--output',
                        default="manager_template.py",
                        dest="output_file",
                        type=str,
                        help="Name of the output file for the template")

    args = vars(parser.parse_args())

    return args


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()

    adapter = args["adapter"].lower()
    valid_schedulers = ['slurm', 'torque', 'pbs', 'lsf']
    scheduler = args['scheduler'].lower()
    if scheduler == "pbs":
        scheduler = "torque"
    if scheduler not in valid_schedulers:
        raise ValueError(
            "Scheduler {} is not implemented in any adapter, must be one of {}".format(scheduler, valid_schedulers)
        )

    test = args["test"]
    test_dict = test_helper[test]

    # Handle Dask adapters
    if adapter == "dask":
        dask_dict, base_dict = dask_templates()
        if scheduler not in dask_dict:
            raise ValueError("Scheduler {} is not known by the Dask template generator. Please choose one of the "
                             "following ({}) and modify through the docs at {}".format(scheduler,
                                                                                       list(dask_dict.keys()),
                                                                                       base_dict["MANAGER_URL"]))
        template_construct = base_script.format(**test_dict, **scheduler_collections[scheduler], **dask_dict[scheduler])

    # Handle Parsl adapters
    elif adapter == "parsl":
        parsl_dict, base_dict = parsl_templates()
        if scheduler not in parsl_dict:
            raise ValueError("Scheduler {} is not known by the parsl template generator. Please choose one of the "
                             "following ({}) and modify through the docs at {}".format(scheduler,
                                                                                       list(parsl_dict.keys()),
                                                                                       base_dict["MANAGER_URL"]))
        template_construct = base_script.format(**test_dict,
                                                **scheduler_collections[scheduler],
                                                **parsl_dict[scheduler])

    # Handle Fireworks adapters
    elif adapter == "fireworks":
        raise NotImplementedError("Fireworks template generators are not yet implemented.")

    elif adapter == "executor":
        raise NotImplementedError("Executor template generators are not yet implemented.")
    else:
        raise KeyError(
            "Unknown adapter type '{}', available options: 'dask' and 'parsl'.".format(adapter))

    with open(args["output_file"], 'w') as f:
        f.write(template_construct)


if __name__ == '__main__':
    main()
