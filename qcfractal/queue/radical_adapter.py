
"""
Queue adapter for Radical-Pilot
"""

import math
import time
import logging
import traceback

import radical.pilot as rp

from typing import Any, Callable, Dict, Hashable, Optional, Tuple

from qcelemental.models import FailedOperation

from .base_adapter import BaseAdapter


# ------------------------------------------------------------------------------
#
class RadicalAdapter(BaseAdapter):
    """An Adapter for Radical-Pilot.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, client: Any, logger: Optional[logging.Logger] = None, **kwargs):
        BaseAdapter.__init__(self, client, logger, **kwargs)

        self._session = client['session']
        self._pmgr    = client['pmgr']
        self._umgr    = client['umgr']
        self._config  = client['config']
        self._tasks   = list()
        self._gc      = list()

        # number of managers we can concurrently run
        self._nmgrs = math.floor(self._config['pd']['cores'] /
                                 self._config['mgr']['cores'])

        print(self._config['pd'])
      # pd = rp.ComputePilotDescription(self._config['pd'])
      # self._pilot = self._pmgr.submit_pilots(pd)

        pd = rp.ComputePilotDescription(self._config['pd'])
        self._pilot = self._pmgr.submit_pilots(pd)
        self._umgr.add_pilots(self._pilot)


    # --------------------------------------------------------------------------
    #
    def __repr__(self):
        return self.client['session'].uid


    # --------------------------------------------------------------------------
    #
    def _submit_task(self, task_spec: Dict[str, Any]) -> Tuple[Hashable, Any]:

        # NOTE: why is this a private method, unlike the others?
        # TODO: bulks

      # # Form run tuple
      # func = self.get_app(task_spec["spec"]["function"])
      # task = func(*task_spec["spec"]["args"], **task_spec["spec"]["kwargs"])

        cuds = list()
        for i in range(0, 128):

            cud = rp.ComputeUnitDescription()
            cud.executable    = '/bin/sleep'
            cud.arguments     = ['2']
            cud.cpu_processes = 1
            cuds.append(cud)

        tasks = self._umgr.submit_units(cuds)
        self._tasks += tasks

        return task_spec["id"], tasks[0].uid


    # --------------------------------------------------------------------------
    #
    def count_running_workers(self) -> int:

        return self._nmgrs


    # --------------------------------------------------------------------------
    #
    def acquire_complete(self) -> Dict[str, Any]:

        # FIXME: limit on stdout
        # TODO : filter out previously reported tasks, GC
        done = {task for task in self._tasks
                              if task.uid not in self._gc and
                                 task.state in rp.FINAL}

        self._gc += [task.uid for task in done]

        return {task.name : task.stdout for task in done}


    # --------------------------------------------------------------------------
    #
    def await_results(self) -> bool:

        self._umgr.wait_units(uids=[task.uid for task in self._tasks])

        # NOTE: should we purge the watch list here?
        self._gc += [task.uid for task in self._tasks]

        return True


    # --------------------------------------------------------------------------
    #
    def close(self) -> bool:

        self._session.close(download=False)
        return True


# ------------------------------------------------------------------------------

