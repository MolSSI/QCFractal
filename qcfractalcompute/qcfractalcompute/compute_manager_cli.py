"""
A command line interface to the qcfractal server.
"""

import argparse
import logging
import signal
import sys

from . import __version__
from .compute_manager import ComputeManager
from .config import read_configuration


def main():
    parser = argparse.ArgumentParser(description="A CLI for a QCFractal QueueManager")
    parser.add_argument("--version", action="version", version=f"{__version__}")
    parser.add_argument("--verbose", action="store_true", help="Print verbose output")
    parser.add_argument("--config", type=str, default=None)

    args = parser.parse_args()

    # Universal formatting for logs
    formatter = logging.Formatter("[%(asctime)s] %(levelname)8s: %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S %Z")

    # Set up a log handler that is used before the logfile is set up
    log_handler = logging.StreamHandler(sys.stdout)
    log_handler.setFormatter(formatter)

    # If the user wants verbose output (particularly about startup of all the commands), then set logging level
    # to be DEBUG
    if args.verbose:
        logging.basicConfig(level="DEBUG", handlers=[log_handler], format="%(levelname)s: %(name)s: %(message)s")
    else:
        logging.basicConfig(level="INFO", handlers=[log_handler], format="%(levelname)s: %(message)s")

    manager_config = read_configuration([args.config])

    # Adjust the logging level to what was in the config
    logging.getLogger().setLevel(manager_config.loglevel)

    if manager_config.logfile:
        print("*" * 10)
        print(f"Logging to file {manager_config.logfile}, logging level {manager_config.loglevel}")
        print("*" * 10)
        log_handler = logging.FileHandler(manager_config.logfile)
        log_handler.setFormatter(formatter)
        logging.getLogger().handlers = [log_handler]

    manager = ComputeManager(manager_config)

    # Catch some signals to gracefully shutdown
    for signame in {"SIGHUP", "SIGINT", "SIGTERM"}:

        def stop(*args, **kwargs):
            logging.info(f"Received signal {signame}, shutting down")
            manager.stop()

        signal.signal(getattr(signal, signame), stop)

    # Blocks until signal
    manager.start()


if __name__ == "__main__":
    main()
