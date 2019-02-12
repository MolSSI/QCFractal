"""
A command line interface to the qcfractal server.
"""

import argparse

import tornado.log

import qcfractal
from . import cli_utils

__all__ = ["main"]


def parse_args():
    parser = argparse.ArgumentParser(description='A CLI for the QCFractal QueueManager.')
    subparsers = parser.add_subparsers(help='QueueManager Backend Type', dest='adapter_type')
    subparsers.required = True

    # Keywords for Dask
    dask_parser = subparsers.add_parser('dask', help='Dask')
    dask_parser.add_argument("--dask-uri", type=str, help="URI of the dask-server")
    dask_parser.add_argument(
        "--local-cluster", action="store_true", help="Start a Dask LocalCluster rather than connect to a scheduler")
    dask_parser.add_argument(
        "--local-workers", type=int, default=None, help="The number of workers for the LocalCluster")

    # Keywords for Fireworks
    fw_parser = subparsers.add_parser('fireworks', help='Fireworks')
    fw_parser.add_argument("--fw-config", type=str, help="A FWConfig file")
    fw_parser.add_argument("--fw-uri", type=str, help="URI of MongoDB server")
    fw_parser.add_argument(
        "--fw-name", type=str, default="qcfractal_fireworks_manager", help="The MongoDB Database to use locally")

    # Keywords for ProcessPoolExecutor
    executor_parser = subparsers.add_parser('executor', help='ProcessPoolExecutor')
    executor_parser.add_argument("--nprocs", type=int, help="The number of process for the executor")

    # FractalClient options
    parser.add_argument(
        "--fractal-uri", type=str, default="localhost:7777", help="FractalServer location to pull from")
    parser.add_argument("-u", "--username", type=str, help="FractalServer username")
    parser.add_argument("-p", "--password", type=str, help="FractalServer password")
    parser.add_argument("--noverify", action="store_true", default=True, help="The logfile prefix to use")

    # QueueManager options
    parser.add_argument(
        "--max-tasks", type=int, default=1000, help="Maximum number of tasks to hold at any given time.")
    parser.add_argument("--cluster-name", type=str, default="unknown", help="The name of the compute cluster to start")
    parser.add_argument("--queue-tag", type=str, help="The queue tag to pull from")
    parser.add_argument("--logfile-prefix", type=str, default=None, help="The prefix of the logfile to write to.")
    parser.add_argument(
        "--update-frequency", type=int, default=15, help="The frequency in seconds to check for complete tasks.")

    # Additional args
    parser.add_argument("--test", action="store_true", help="Boot and run a short test suite to validate setup")
    parser.add_argument("--rapidfire", action="store_true", help="Boot and run jobs until complete")
    parser.add_argument("--config-file", type=str, default=None, help="A configuration file to use")
    args = vars(parser.parse_args())
    if args["config_file"] is not None:
        data = cli_utils.read_config_file(args["config_file"])
        args = cli_utils.argparse_config_merge(parser, args, data, parser_default=[args["adapter_type"]])

    if args["rapidfire"] and args["test"]:
        raise KeyError("Can only either select 'rapidfire' or 'test' options, but not both.")

    return args


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()

    exit_callbacks = []

    # Handle Dask adapters
    if args["adapter_type"] == "dask":
        dd = cli_utils.import_module("distributed")

        if args["local_cluster"]:
            # Build localcluster and exit callbacks
            queue_client = dd.Client(threads_per_worker=1, n_workers=args["local_workers"])
        else:
            if args["dask_uri"] is None:
                raise KeyError("A 'dask-uri' must be specified.")
            queue_client = dd.Client(args["dask_uri"])

    # Handle Fireworks adapters
    elif args["adapter_type"] == "fireworks":

        # Check option conflicts
        num_options = sum(args[x] is not None for x in ["fw_config", "fw_uri"])
        if num_options == 0:
            args["fw_uri"] = "mongodb://localhost:27017"
        elif num_options != 1:
            raise KeyError("Can only provide a single URI or config_file for Fireworks.")

        fireworks = cli_utils.import_module("fireworks")

        if args["fw_uri"] is not None:
            queue_client = fireworks.LaunchPad(args["fw_uri"], name=args["fw_name"])
        elif args["fw_config"] is not None:
            queue_client = fireworks.LaunchPad.from_file(args["fw_config"])
        else:
            raise KeyError("A URI or config_file must be specified.")

    elif args["adapter_type"] == "executor":

        from concurrent.futures import ProcessPoolExecutor

        queue_client = ProcessPoolExecutor(max_workers=args["nprocs"])

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

    # Build out the manager itself
    manager = qcfractal.queue.QueueManager(
        client,
        queue_client,
        max_tasks=args["max_tasks"],
        queue_tag=args["queue_tag"],
        cluster=args["cluster_name"],
        update_frequency=args["update_frequency"])

    # Add exit callbacks
    for cb in exit_callbacks:
        manager.add_exit_callback(cb[0], *cb[1], **cb[2])

    # Either startup the manager or run until complete
    if args["rapidfire"]:
        manager.await_results()
    elif args["test"]:
        success = manager.test()
        if success is False:
            raise ValueError("Testing was not successful, failing.")
    else:

        cli_utils.install_signal_handlers(manager.loop, manager.stop)

        # Blocks until keyboard interupt
        manager.start()


if __name__ == '__main__':
    main()
