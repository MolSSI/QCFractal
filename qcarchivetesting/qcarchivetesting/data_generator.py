from __future__ import annotations

import multiprocessing
from typing import TYPE_CHECKING

from qcfractal.process_runner import ProcessBase
from qcfractalcompute import ComputeManager, _initialize_signals_process_pool

if TYPE_CHECKING:
    from qcfractal.config import FractalConfig


def process_result(result):
    return result.dict()


class DataGeneratorManager(ComputeManager):
    def __init__(self, worker_pool, fractal_uri, manager_name, result_queue):
        ComputeManager.__init__(self, worker_pool, fractal_uri=fractal_uri, manager_name=manager_name)

        # Maps task id to full task
        self._task_map = {}

        self._result_queue = result_queue

    def postprocess_results(self, results):
        # results is a dict of task_id to result
        for task_id, result in results.items():
            # Return full task + full result (as dict)
            self._result_queue.put((self._task_map[task_id], result.dict()))

    def preprocess_new_tasks(self, new_tasks):
        for task in new_tasks:
            # Store the full task by task id
            self._task_map[task["id"]] = task


class DataGeneratorComputeProcess(ProcessBase):
    """
    Runs  a compute manager for data generation a separate process
    """

    def __init__(self, qcf_config: FractalConfig, compute_workers: int = 2):
        self._qcf_config = qcf_config
        self._compute_workers = compute_workers

        self._result_queue = multiprocessing.Queue()

        # Don't initialize the worker pool here. It must be done in setup(), because
        # that is run in the separate process

    def setup(self) -> None:
        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}"

        self._worker_pool = multiprocessing.Pool(
            processes=self._compute_workers, initializer=_initialize_signals_process_pool
        )
        self._queue_manager = DataGeneratorManager(
            self._worker_pool, fractal_uri=uri, manager_name="data_generator_compute", result_queue=self._result_queue
        )

    def run(self) -> None:
        self._queue_manager.start()

    def interrupt(self) -> None:
        self._queue_manager.stop()
        self._worker_pool.terminate()
        self._worker_pool.join()

    def get_data(self):
        # Returns list of iterable (task, result)
        data = []

        while not self._result_queue.empty():
            d = self._result_queue.get(False)
            data.append(d)

        return data
