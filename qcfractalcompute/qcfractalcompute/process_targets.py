from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
import threading
import weakref
from typing import Optional

from qcfractalcompute import ComputeManager
from qcfractalcompute.config import FractalComputeConfig


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


class QCFComputeThread:
    def __init__(
        self,
        compute_config: FractalComputeConfig,
    ):
        self._compute_config = compute_config
        self._compute: Optional[ComputeManager] = None
        self._compute_thread = None
        self._finalizer = None

    # Classmethod because finalizer can't handle bound methods
    @classmethod
    def _stop(cls, compute, compute_thread):
        if compute is not None:
            compute.stop()
            compute_thread.join()

    def start(self, initialized_event: Optional[threading.Event] = None) -> None:
        if self._compute is not None:
            raise RuntimeError("Compute manager already started")

        self._compute = ComputeManager(self._compute_config)

        # We use daemon=True
        # This means that the main python process can exit, calling various destructors
        # and finalizers (rather than waiting for those threads to finish before doing so)
        self._compute_thread = threading.Thread(target=self._compute.start, daemon=True)
        self._compute_thread.start()

        if initialized_event is not None:
            initialized_event.set()

        self._finalizer = weakref.finalize(
            self,
            self._stop,
            self._compute,
            self._compute_thread,
        )

    def stop(self) -> None:
        if self._finalizer is not None:
            self._finalizer()

        self._compute = None
        self._compute_thread = None

    def is_alive(self) -> bool:
        if self._compute_thread is None:
            return False
        return self._compute_thread.is_alive()
