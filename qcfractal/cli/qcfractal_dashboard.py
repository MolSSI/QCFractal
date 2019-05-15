"""
A command line interface to the qcfractal server.
"""

import argparse

from qcfractal.dashboard.start_dashboard import dashboard_app

from . import cli_utils


def parse_args():
    parser = argparse.ArgumentParser(description='A CLI for the QCFractalDashboard.')

    parser.add_argument("database_name", type=str, help="The name of the database to use with the storage socket")
    parser.add_argument("--database-uri", type=str, default="mongodb://localhost", help="The database URI to use")
    parser.add_argument("--config-file", type=str, default=None, help="A configuration file to use")

    args = vars(parser.parse_args())
    if args["config_file"] is not None:
        data = cli_utils.read_config_file(args["config_file"])
        args = cli_utils.argparse_config_merge(parser, args, data)

    return args


def main(args=None):

    # Grab CLI args if not present
    if args is None:
        args = parse_args()

    dashboard_app.server.config["DATABASE_URI"] = args["database_uri"]
    dashboard_app.server.config["DATABASE_NAME"] = args["database_name"]

    dashboard_app.run_server(debug=True)

if __name__ == '__main__':
    main()
