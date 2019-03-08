"""
A command line interface to the qcfractal server.
"""

import argparse

import qcfractal
import tornado.log
import qcengine as qcng

from . import cli_utils

__all__ = ["main"]


def parse_args():
    parser = argparse.ArgumentParser(
        description='A CLI for a QCFractal QueueManager with a ProcessPoolExecutor backend.')

    # Keywords for ProcessPoolExecutor
    executor = parser.add_argument_group('Executor settings')
    executor.add_argument(
        "--ntasks",
        required=True,
        type=int,
        help="The number of simultaneous tasks for the executor to run, resources will be divided evenly.")
    executor.add_argument("--cores", type=int, help="The number of process for the executor")
    executor.add_argument("--memory", type=int, help="The total amount of memory on the system in GB")

    # FractalClient options
    server = parser.add_argument_group('FractalServer connection settings')
    server.add_argument(
        "--fractal-uri", type=str, default="localhost:7777", help="FractalServer location to pull from")
    server.add_argument("-u", "--username", type=str, help="FractalServer username")
    server.add_argument("-p", "--password", type=str, help="FractalServer password")
    server.add_argument("--noverify", action="store_true", default=True, help="The logfile prefix to use")

    # QueueManager options
    manager = parser.add_argument_group("QueueManager settings")
    manager.add_argument(
        "--max-tasks", type=int, default=1000, help="Maximum number of tasks to hold at any given time.")
    manager.add_argument(
        "--cluster-name", type=str, default="unknown", help="The name of the compute cluster to start")
    manager.add_argument("--queue-tag", type=str, help="The queue tag to pull from")
    manager.add_argument("--logfile-prefix", type=str, default=None, help="The prefix of the logfile to write to.")
    manager.add_argument(
        "--update-frequency", type=int, default=15, help="The frequency in seconds to check for complete tasks.")

    # Additional args
    optional = parser.add_argument_group('Optional Settings')
    optional.add_argument("--test", action="store_true", help="Boot and run a short test suite to validate setup")
    optional.add_argument("--config-file", type=str, default=None, help="A configuration file to use")

    args = vars(parser.parse_args())
    if args["config_file"] is not None:
        data = cli_utils.read_config_file(args["config_file"])
        args = cli_utils.argparse_config_merge(parser, args, data, parser_default=[args["adapter_type"]])

    return args


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()

    exit_callbacks = []
    args["adapter_type"] = "executor"

    # Handle Dask adapters
    # if args["adapter_type"] == "dask":
    #     dd = cli_utils.import_module("distributed")

    #     if args["local_cluster"]:
    #         # Build localcluster and exit callbacks
    #         queue_client = dd.Client(threads_per_worker=1, n_workers=args["local_workers"])
    #     else:
    #         if args["dask_uri"] is None:
    #             raise KeyError("A 'dask-uri' must be specified.")
    #         queue_client = dd.Client(args["dask_uri"])

    # # Handle Fireworks adapters
    # elif args["adapter_type"] == "fireworks":

    #     # Check option conflicts
    #     num_options = sum(args[x] is not None for x in ["fw_config", "fw_uri"])
    #     if num_options == 0:
    #         args["fw_uri"] = "mongodb://localhost:27017"
    #     elif num_options != 1:
    #         raise KeyError("Can only provide a single URI or config_file for Fireworks.")

    #     fireworks = cli_utils.import_module("fireworks")

    #     if args["fw_uri"] is not None:
    #         queue_client = fireworks.LaunchPad(args["fw_uri"], name=args["fw_name"])
    #     elif args["fw_config"] is not None:
    #         queue_client = fireworks.LaunchPad.from_file(args["fw_config"])
    #     else:
    #         raise KeyError("A URI or config_file must be specified.")

    if args["cores"] is None:
        args["cores"] = qcng.config.get_global("ncores")

    if args["memory"] is None:
        args["memory"] = qcng.config.get_global("memory")

    if args["adapter_type"] == "executor":

        from concurrent.futures import ProcessPoolExecutor

        queue_client = ProcessPoolExecutor(max_workers=args["ntasks"])

    else:
        raise KeyError(
            "Unknown adapter type '{}', available options: 'fireworks', 'dask', .".format(args["adapter_type"]))

    # Quick logging
    if args["logfile_prefix"] is not None:
        tornado.options.options['log_file_prefix'] = logfile_prefix
    tornado.log.enable_pretty_logging()

    # Build the client
    if args["test"]:
        client = None
    else:
        client = qcfractal.interface.FractalClient(
            args["fractal_uri"], username=args["username"], password=args["password"], verify=(not args["noverify"]))

    # Figure out per-task data
    cores_per_task = args["cores"] // args["ntasks"]
    memory_per_task = args["memory"] / args["ntasks"]
    if cores_per_task < 1:
        raise ValueError("Cores per task must be larger than one!")

    # Build out the manager itself
    manager = qcfractal.queue.QueueManager(
        client,
        queue_client,
        max_tasks=args["max_tasks"],
        queue_tag=args["queue_tag"],
        cluster=args["cluster_name"],
        update_frequency=args["update_frequency"],
        cores_per_task=cores_per_task,
        memory_per_task=memory_per_task)

    # Add exit callbacks
    for cb in exit_callbacks:
        manager.add_exit_callback(cb[0], *cb[1], **cb[2])

    # Either startup the manager or run until complete
    if args["test"]:
        success = manager.test()
        if success is False:
            raise ValueError("Testing was not successful, failing.")
    else:

        cli_utils.install_signal_handlers(manager.loop, manager.stop)

        # Blocks until keyboard interupt
        manager.start()


if __name__ == '__main__':
    main()
