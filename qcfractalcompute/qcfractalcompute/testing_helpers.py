from __future__ import annotations

import os
import tempfile
import threading
import weakref
from typing import Dict, List, Optional, Any

import parsl

from qcarchivetesting.testing_classes import _activated_manager_programs
from qcfractal.components.optimization.testing_helpers import submit_test_data as submit_opt_test_data
from qcfractal.components.singlepoint.testing_helpers import submit_test_data as submit_sp_test_data
from qcfractal.config import FractalConfig
from qcfractal.db_socket import SQLAlchemySocket
from qcfractalcompute.apps.models import AppTaskResult
from qcfractalcompute.compress import compress_result
from qcfractalcompute.compute_manager import ComputeManager
from qcfractalcompute.config import FractalComputeConfig, FractalServerSettings, LocalExecutorConfig
from qcportal.all_results import AllResultTypes, FailedOperation
from qcportal.record_models import PriorityEnum, RecordTask
from qcportal.utils import update_nested_dict

failed_op = FailedOperation(
    input_data=None,
    error={"error_type": "internal_error", "error_message": "This is a test error message"},
)


class MockTestingAppManager:
    def all_program_info(self, executor_label: Optional[str] = None) -> Dict[str, List[str]]:
        return _activated_manager_programs


class MockTestingComputeManager(ComputeManager):
    def __init__(
        self,
        qcf_config: FractalConfig,
        result_data: Optional[Dict[int, AllResultTypes]] = None,
        additional_manager_config: Optional[Dict[str, Any]] = None,
    ):
        self._qcf_config = qcf_config

        host = self._qcf_config.api.host
        port = self._qcf_config.api.port
        uri = f"http://{host}:{port}"

        # Not we set the update_frequency to be quite a bit less than the heartbeat_frequency
        # from the server. This allows us to test dropping deferred tasks without having the manager
        # die due to missed heartbeats
        tmpdir = tempfile.TemporaryDirectory()
        parsl_run_dir = os.path.join(tmpdir.name, "parsl_run_dir")
        compute_scratch_dir = os.path.join(tmpdir.name, "compute_scratch_dir")
        os.makedirs(parsl_run_dir, exist_ok=True)
        os.makedirs(compute_scratch_dir, exist_ok=True)

        config_dict = dict(
            base_folder=tmpdir.name,
            parsl_run_dir=parsl_run_dir,
            cluster="mock_compute",
            update_frequency=1,
            update_frequency_jitter=0.0,
            server=FractalServerSettings(
                fractal_uri=uri,
                verify=False,
            ),
            executors={
                "local": LocalExecutorConfig(
                    cores_per_worker=1,
                    memory_per_worker=1,
                    max_workers=1,
                    compute_tags=["*"],
                    scratch_directory=compute_scratch_dir,
                )
            },
        )

        if additional_manager_config:
            config_dict = update_nested_dict(config_dict, additional_manager_config)

        self._compute_config = FractalComputeConfig(**config_dict)

        # Set the app manager already
        self.app_manager = MockTestingAppManager()
        ComputeManager.__init__(self, self._compute_config)

        def cleanup(d):
            d.cleanup()

        weakref.finalize(self, cleanup, tmpdir)

        # Shorten the timeout on the client for testing
        self.client._timeout = 2

        self._result_data = result_data

    # We have an executor and everything running, but we short-circuit the actual compute
    def _submit_tasks(self, executor_label: str, tasks: List[RecordTask]):
        # A mock app that just returns the result data given to it after two seconds
        @parsl.python_app(data_flow_kernel=self.dflow_kernel)
        def _mock_app(result: Optional[AllResultTypes]) -> AppTaskResult:
            import time

            time.sleep(2)

            if result is None:
                return AppTaskResult(success=False, walltime=2.0, result_compressed=compress_result(failed_op.dict()))
            else:
                return AppTaskResult(
                    success=result.success, walltime=2.0, result_compressed=compress_result(result.dict())
                )

        for task in tasks:
            self._record_id_map[task.id] = task.record_id

            if self._result_data:
                task_future = _mock_app(self._result_data.get(task.record_id, None))
            else:
                task_future = _mock_app(None)

            self._task_futures[executor_label][task.id] = task_future


class QCATestingComputeThread:
    """
    Runs a compute manager in a separate process
    """

    def __init__(
        self,
        qcf_config: FractalConfig,
        result_data: Dict[int, AllResultTypes] = None,
        additional_manager_config: Optional[Dict[str, Any]] = None,
    ):
        self._qcf_config = qcf_config
        self._result_data = result_data

        self._additional_manager_config = additional_manager_config

        self._compute: Optional[MockTestingComputeManager] = None
        self._compute_thread = None

        self._finalizer = None

    # Classmethod because finalizer can't handle bound methods
    @classmethod
    def _stop(cls, compute, compute_thread):
        if compute is not None:
            compute.stop()
            compute_thread.join()

    def start(self, manual_updates) -> None:
        if self._compute is not None:
            raise RuntimeError("Compute manager already started")
        self._compute = MockTestingComputeManager(self._qcf_config, self._result_data, self._additional_manager_config)
        self._compute_thread = threading.Thread(
            target=self._compute.start, kwargs={"manual_updates": manual_updates}, daemon=True
        )
        self._compute_thread.start()

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


def populate_db(storage_socket: SQLAlchemySocket):
    # explicitly load enough so we have to do chunking on the return
    id_0, result_data_0 = submit_opt_test_data(storage_socket, "opt_psi4_methane_sometraj", "tag0", PriorityEnum.normal)
    id_1, result_data_1 = submit_sp_test_data(storage_socket, "sp_psi4_water_gradient", "tag1", PriorityEnum.high)
    id_2, result_data_2 = submit_sp_test_data(storage_socket, "sp_psi4_water_hessian", "tag2", PriorityEnum.high)
    id_3, result_data_3 = submit_sp_test_data(storage_socket, "sp_psi4_peroxide_energy_wfn", "tag3", PriorityEnum.high)
    id_4, result_data_4 = submit_sp_test_data(storage_socket, "sp_rdkit_water_energy", "tag4", PriorityEnum.normal)
    id_5, result_data_5 = submit_sp_test_data(storage_socket, "sp_psi4_benzene_energy_2", "tag5", PriorityEnum.normal)
    id_6, result_data_6 = submit_sp_test_data(storage_socket, "sp_psi4_water_energy", "tag6", PriorityEnum.normal)
    all_id = [id_0, id_1, id_2, id_3, id_4, id_5, id_6]

    result_data = {
        id_0: result_data_0,
        id_1: result_data_1,
        id_2: result_data_2,
        id_3: result_data_3,
        id_4: result_data_4,
        id_5: result_data_5,
        id_6: result_data_6,
    }

    return all_id, result_data
