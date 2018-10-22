"""
A command line interface to the qcfractal server.
"""

import argparse

from . import cli_utils
import qcfractal

parser = argparse.ArgumentParser(description='A CLI for the QCFractalServer.')

manager = parser.add_argument_group('QueueManager Settings (optional)')
manager_exclusive = manager.add_mutually_exclusive_group()
manager_exclusive.add_argument(
    "--dask-manager", action="store_true", help="Creates a QueueManager using a Dask LocalCluster on the server")
manager_exclusive.add_argument(
    "--fireworks-manager",
    action="store_true",
    help="Creates a QueueManager using Fireworks on the server (name + '_fireworks_queue')")

server = parser.add_argument_group('QCFractalServer Settings')
server.add_argument("name", type=str, help="The name of the FractalServer and its associated database")
server.add_argument("--log-prefix", type=str, default=None, help="The logfile prefix to use")
server.add_argument("--port", type=int, default=7777, help="The server port")
server.add_argument("--security", type=str, default=None, choices=[None, "local"], help="The security protocol to use")
server.add_argument("--database-uri", type=str, default="mongodb://localhost", help="The database URI to use")
server.add_argument("--tls-cert", type=str, default=None, help="Certificate file for TLS (in PEM format)")
server.add_argument("--tls-key", type=str, default=None, help="Private key file for TLS (in PEM format)")
server.add_argument("--config-file", type=str, default=None, help="A configuration file to use")

parser._action_groups.reverse()

args = vars(parser.parse_args())
if args["config_file"] is not None:
    data = cli_utils.read_config_file(args["config_file"])

    args = cli_utils.argparse_config_merge(parse, args, data)


def main():

    # Handle SSL
    ssl_certs = sum(args[x] is not None for x in ["tls_key", "tls_cert"])
    if ssl_certs == 0:
        ssl_options = None
    elif ssl_certs == 2:
        ssl_options = {"crt": args["tls_cert"], "key": args["tls_key"]}
    else:
        raise KeyError("Both tls-cert and tls-key must be passed in.")

    # Handle Adapters/QueueManagers
    exit_callbacks = []

    # Build an optional adapter
    if args["dask_manager"]:
        dd = cli_utils.import_module("distributed")

        # Build localcluster and exit callbacks
        local_cluster = dd.LocalCluster(threads_per_worker=1)
        adapter = dd.Client(local_cluster)
        exit_callbacks.append([adapter.close, (), {}])
        exit_callbacks.append([local_cluster.scale_down, (local_cluster.workers, ), {}])
        exit_callbacks.append([local_cluster.close, (4, ), {}])

    elif args["fireworks_manager"]:
        fw = cli_utils.import_module("fireworks")

        # Build Fireworks client
        name = args["name"] + "_fireworks_queue"
        adapter = fw.LaunchPad(host=args["database_uri"], name=name)
        adapter.reset(None, require_password=False)  # Leave cap on reset
        exit_callbacks.append(
            [adapter.reset, (None, ), {
                "require_password": False,
                "max_reset_wo_password": int(1e8)
            }])

    else:
        adapter = None

    # Build the server itself
    server = qcfractal.FractalServer(
        port=args["port"],
        security=args["security"],
        ssl_options=ssl_options,
        storage_uri=args["database_uri"],
        storage_project_name=args["name"],
        logfile_prefix=args["log_prefix"],
        queue_socket=adapter)

    # Print Queue Manager data
    if args["dask_manager"]:
        server.logger.info("\nDask QueueManager initialized: {}\n".format(str(adapter)))
    elif args["fireworks_manager"]:
        server.logger.info("\nFireworks QueueManager initialized: \n"
                           "    Host: {}, Name: {}\n".format(adapter.host, adapter.name))

    # Add exit callbacks
    for cb in exit_callbacks:
        server.add_exit_callback(cb[0], *cb[1], **cb[2])

    server.start()


if __name__ == '__main__':
    main()
