"""
A command line interface to the qcfractal server.
"""

import argparse
import logging
import signal

from . import __version__
from .compute_manager import ComputeManager
from .config import read_configuration


def main():

    logging.basicConfig()

    parser = argparse.ArgumentParser(description="A CLI for a QCFractal QueueManager")
    parser.add_argument("--version", action="version", version=f"{__version__}")
    parser.add_argument("--config", type=str, default=None)

    args = parser.parse_args()

    manager_config = read_configuration([args.config])

    # Adjust the logging level to what was in the config
    logging.getLogger().setLevel(manager_config.loglevel)

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
