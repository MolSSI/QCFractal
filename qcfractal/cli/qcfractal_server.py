"""
A command line interface to the qcfractal server.
"""

import argparse
import qcfractal

from . import cli_utils

parser = argparse.ArgumentParser(description='A CLI for the QCFractalServer.')
parser.add_argument(
    "--name", type=str, help="The name of the FractalServer and its associated database", required=True)
parser.add_argument("--logfile", type=str, default=None, help="The logfile to use")
parser.add_argument("--port", type=int, default=7777, help="The server port")
parser.add_argument("--security", type=str, default=None, help="The security protocol to use")
parser.add_argument("--database-uri", type=str, default="localhost:27017", help="The database URI to use")
parser.add_argument("--tls-cert", type=str, default=None, help="Certificate file for TLS (in PEM format)")
parser.add_argument("--tls-key", type=str, default=None, help="Private key file for TLS (in PEM format)")
parser.add_argument("--config-file", type=str, default=None, help="A configuration file to use")

args = vars(parser.parse_args())
if args["config_file"] is not None:
    data = cli_utils.read_config_file(args["config_file"])
    diff = data.keys() - args.keys()
    if diff:
        raise argparse.ArgumentError(None,
                                     "Unknown arguments found in configuration file: {}.".format(", ".join(diff)))

    # Overwrite config args with config_file
    # This isnt quite what we want, CLI should take precedence over file?
    args = {**args, **data}


def main():
    print(args)

    # Handle SSL
    ssl_certs = sum(args[x] is not None for x in ["tls_key", "tls_cert"])
    if ssl_certs == 0:
        ssl_options = None
    elif ssl_certs == 2:
        ssl_options = {"crt": args["tls_cert"], "key": args["tls_key"]}
    else:
        raise KeyError("Both tls-cert and tls-key must be passed in.")

    # lpad = fireworks.LaunchPad.from_file("fw_lpad.yaml")
    server = qcfractal.FractalServer(
        port=args["port"], security=args["security"], ssl_options=ssl_options, logfile_name=args["logfile"])

    server.start()


if __name__ == '__main__':
    main()
