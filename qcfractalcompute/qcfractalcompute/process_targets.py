from __future__ import annotations

import logging
import logging.handlers
import multiprocessing

from qcfractalcompute import ComputeManager
from qcfractalcompute.config import FractalComputeConfig
from typing import Optional


def compute_process(
    compute_config: FractalComputeConfig,
    logging_queue: multiprocessing.Queue,
    initialized_event: Optional[multiprocessing.Event] = None,
) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)
    logger.setLevel(compute_config.loglevel)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    compute = ComputeManager(compute_config)
    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    def signal_handler(signum, frame):
        compute.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if initialized_event is not None:
        initialized_event.set()

    try:
        compute.start()
    except KeyboardInterrupt:  # Swallow ugly output on CTRL-C
        pass
    finally:
        logging_queue.close()
        logging_queue.join_thread()
