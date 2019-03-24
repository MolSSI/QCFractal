"""
A command line interface to the qcfractal server.
"""

import argparse

import qcfractal

from . import cli_utils


def parse_args():
    parser = argparse.ArgumentParser(description='A CLI for the QCFractalServer.')

    manager = parser.add_argument_group('Local QueueManager Settings (optional)')
    # This option defaults to None if option not present, -1 if present, or value if provided
    manager.add_argument(
        '--local-manager',
        const=-1,
        default=None,
        action='store',
        nargs='?',
        type=int,
        help='Creates a local pool QueueManager')

    manager = parser.add_argument_group('Manager Settings')
    manager.add_argument("--heartbeat-frequency", type=int, default=300, help="The manager heartbeat frequency.")

    security = parser.add_argument_group('Security Settings')
    security.add_argument(
        "--security", type=str, default=None, choices=[None, "local"], help="The security protocol to use")
    security.add_argument(
        "--allow-read", type=bool, default=True, help="Allow read-only queries or not if security is active.")
    security.add_argument("--disable-ssl", type=bool, default=False, help="Disables SSL if present, if False a SSL cert will be created for you")
    security.add_argument("--tls-cert", type=str, default=None, help="Certificate file for TLS (in PEM format)")
    security.add_argument("--tls-key", type=str, default=None, help="Private key file for TLS (in PEM format)")

    general = parser.add_argument_group('General Settings')
    general.add_argument("database_name", type=str, help="The name of the database to use with the storage socket")
    general.add_argument("--server-name", type=str, default="QCFractal Server", help="The server name to broadcast")
    general.add_argument("--query-limit", type=int, default=1000, help="The maximum query size to server")
    general.add_argument("--log-prefix", type=str, default=None, help="The logfile prefix to use")
    general.add_argument("--database-uri", type=str, default="mongodb://localhost", help="The database URI to use")
    general.add_argument("--port", type=int, default=7777, help="The server port")
    general.add_argument("--compress-response", type=bool, default=True, help="Compress the response or not")
    general.add_argument("--config-file", type=str, default=None, help="A configuration file to use")
    general.add_argument("--start-periodics", type=bool, default=True, help="Start the periodic calls or not, always recommended unless running fractal-server behind a proxy")

    parser._action_groups.reverse()

    args = vars(parser.parse_args())
    if args["config_file"] is not None:
        data = cli_utils.read_config_file(args["config_file"])
        args = cli_utils.argparse_config_merge(parser, args, data)

    return args


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()

    # Handle SSL
    if args["disable_ssl"]:
        ssl_options = False
    else:
        ssl_certs = sum(args[x] is not None for x in ["tls_key", "tls_cert"])
        if ssl_certs == 0:
            ssl_options = True
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
        name=args["server_name"],
        port=args["port"],
        compress_response=args["compress_response"],

        # Security
        security=args["security"],
        allow_read=args["allow_read"],
        ssl_options=ssl_options,

        # Database
        storage_uri=args["database_uri"],
        storage_project_name=args["database_name"],
        query_limit=args["query_limit"],

        # Log options
        logfile_prefix=args["log_prefix"],

        # Queue options
        heartbeat_frequency=args["heartbeat_frequency"],
        queue_socket=adapter)

    # Add exit callbacks
    for cb in exit_callbacks:
        server.add_exit_callback(cb[0], *cb[1], **cb[2])

    # Register closing
    cli_utils.install_signal_handlers(server.loop, server.stop)

    # Blocks until keyboard interupt
    server.start(start_periodics=args["start_periodics"])


if __name__ == '__main__':
    main()
