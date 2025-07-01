from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
from typing import Optional

from qcfractal.config import FractalConfig
from qcfractal.flask_app.waitress_app import FractalWaitressApp
from qcfractal.job_runner import FractalJobRunner
from qcfractalcompute import ComputeManager
from qcfractalcompute.config import FractalComputeConfig


def api_process(
    qcf_config: FractalConfig,
    logging_queue: multiprocessing.Queue,
    finished_queue: Optional[multiprocessing.Queue],
) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)
    logger.setLevel(qcf_config.loglevel)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    api = FractalWaitressApp(qcf_config, finished_queue=finished_queue)

    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    def signal_handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        api.run()
    except KeyboardInterrupt:  # Swallow ugly output on CTRL-C
        pass
    finally:
        logging_queue.close()
        logging_queue.join_thread()


def compute_process(compute_config: FractalComputeConfig, logging_queue: multiprocessing.Queue) -> None:
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

    try:
        compute.start()
    except KeyboardInterrupt:  # Swallow ugly output on CTRL-C
        pass
    finally:
        logging_queue.close()
        logging_queue.join_thread()


def job_runner_process(
    qcf_config: FractalConfig, logging_queue: multiprocessing.Queue, finished_queue: Optional[multiprocessing.Queue]
) -> None:
    import signal

    qh = logging.handlers.QueueHandler(logging_queue)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(qh)
    logger.setLevel(qcf_config.loglevel)

    early_stop = False

    def signal_handler(signum, frame):
        nonlocal early_stop
        early_stop = True

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    job_runner = FractalJobRunner(qcf_config, finished_queue=finished_queue)

    if early_stop:
        logging_queue.close()
        logging_queue.join_thread()
        return

    def signal_handler(signum, frame):
        job_runner.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        job_runner.start()
    except KeyboardInterrupt:  # Swallow ugly output on CTRL-C
        pass
    finally:
        logging_queue.close()
        logging_queue.join_thread()
