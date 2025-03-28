from __future__ import annotations

import queue
import tempfile
import threading
import weakref
from typing import TYPE_CHECKING

from qcfractalcompute import ComputeManager
from qcfractalcompute.config import FractalComputeConfig, FractalServerSettings, LocalExecutorConfig

if TYPE_CHECKING:
    from typing import List, Dict, Any
    from qcportal.record_models import RecordTask
    from qcfractal.config import FractalConfig
    from qcfractalcompute.apps.models import AppTaskResult


def clean_conda_env(d: Dict[str, Any]):
    """
    Remove the bulky conda env info from provenances
    """
    if "conda_environment" in d:
        del d["conda_environment"]

    for v in d.values():
        if isinstance(v, dict):
            clean_conda_env(v)
        elif isinstance(v, list):
            for x in v:
                if isinstance(x, dict):
                    clean_conda_env(x)


class DataGeneratorManager(ComputeManager):
    def __init__(self, qcf_config: FractalConfig, result_queue: queue.Queue, n_workers: int = 2):
        self._qcf_config = qcf_config
        self._result_queue = result_queue
        self._record_id_map = {}  # Maps task id to record id

        # Maps task id to full task
        self._task_map = {}

        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}"

        tmpdir = tempfile.TemporaryDirectory()
        self._compute_config = FractalComputeConfig(
            base_folder=tmpdir.name,
            cluster="datagenerator_compute",
            update_frequency=10,
            server=FractalServerSettings(
                fractal_uri=uri,
                verify=False,
            ),
            executors={
                "local": LocalExecutorConfig(
                    scratch_directory=tmpdir.name,
                    cores_per_worker=1,
                    memory_per_worker=1,
                    max_workers=n_workers,
                    compute_tags=["*"],
                )
            },
        )
        ComputeManager.__init__(self, self._compute_config)

        def cleanup(d):
            d.cleanup()

        weakref.finalize(self, cleanup, tmpdir)

    def postprocess_results(self, results: Dict[int, AppTaskResult]):
        for task_id, app_result in results.items():
            # Return full task + full result (as dict)
            r_dict = app_result.result
            clean_conda_env(r_dict)
            self._result_queue.put((self._task_map[task_id], r_dict))

    def preprocess_new_tasks(self, new_tasks: List[RecordTask]):
        for task in new_tasks:
            # Store the full task by task id
            self._task_map[task.id] = task


class DataGeneratorComputeThread:
    def __init__(self, qcf_config: FractalConfig, n_workers: int = 2):
        self._qcf_config = qcf_config

        self._result_queue = queue.Queue()

        self._compute = DataGeneratorManager(self._qcf_config, self._result_queue, n_workers)
        self._compute_thread = threading.Thread(target=self._compute.start, daemon=True)

        self._finalizer = weakref.finalize(
            self,
            self._stop,
            self._compute,
            self._compute_thread,
        )

        self._compute_thread.start()

    @classmethod
    def _stop(cls, compute, compute_thread):
        if compute is not None:
            compute.stop()
            compute_thread.join()

    def stop(self) -> None:
        self._finalizer()

    def get_data(self) -> List[tuple[RecordTask, Dict[str, Any]]]:
        # Returns list of iterable (task, result)
        data = []

        while not self._result_queue.empty():
            d = self._result_queue.get(False)
            data.append(d)

        return data
