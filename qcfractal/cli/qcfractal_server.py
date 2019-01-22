"""
A command line interface to the qcfractal server.
"""

import argparse

import qcfractal
from . import cli_utils


def parse_args():
    parser = argparse.ArgumentParser(description='A CLI for the QCFractalServer.')

    manager = parser.add_argument_group('QueueManager Settings (optional)')
    # This option defaults to None if option not present, -1 if present, or value if provided
    manager.add_argument(
        '--local-manager',
        const=-1,
        default=None,
        action='store',
        nargs='?',
        type=int,
        help='Creates a local pool QueueManager')

    server = parser.add_argument_group('QCFractalServer Settings')
    server.add_argument("name", type=str, help="The name of the FractalServer and its associated database")
    server.add_argument("--log-prefix", type=str, default=None, help="The logfile prefix to use")
    server.add_argument("--port", type=int, default=7777, help="The server port")
    server.add_argument(
        "--security", type=str, default=None, choices=[None, "local"], help="The security protocol to use")
    server.add_argument("--database-uri", type=str, default="mongodb://localhost", help="The database URI to use")
    server.add_argument("--tls-cert", type=str, default=None, help="Certificate file for TLS (in PEM format)")
    server.add_argument("--tls-key", type=str, default=None, help="Private key file for TLS (in PEM format)")
    server.add_argument("--config-file", type=str, default=None, help="A configuration file to use")
    server.add_argument("--heartbeat-frequency", type=int, default=300, help="The manager heartbeat frequency.")

    parser._action_groups.reverse()

    args = vars(parser.parse_args())
    if args["config_file"] is not None:
        data = cli_utils.read_config_file(args["config_file"])

        args = cli_utils.argparse_config_merge(parse, args, data)

    return args


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()

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
    if args["local_manager"]:
        ncores = args["local_manager"]
        if ncores == -1:
            ncores = None

        from concurrent.futures import ProcessPoolExecutor

        adapter = ProcessPoolExecutor(max_workers=ncores)

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
        heartbeat_frequency=args["heartbeat_frequency"],
        queue_socket=adapter)

    # Add exit callbacks
    for cb in exit_callbacks:
        server.add_exit_callback(cb[0], *cb[1], **cb[2])

    # Register closing
    cli_utils.install_signal_handlers(server.loop, server.stop)

    # Blocks until keyboard interupt
    server.start()


if __name__ == '__main__':
    main()
