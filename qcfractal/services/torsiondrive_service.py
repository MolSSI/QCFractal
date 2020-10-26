"""
Wraps geometric procedures
"""

import io
import copy
import json
import contextlib
from typing import Any, Dict, List

import numpy as np

from ..extras import find_module
from ..interface.models import TorsionDriveRecord
from .service_util import BaseService, TaskManager

__all__ = ["TorsionDriveService"]

__td_api = find_module("torsiondrive")


def _check_td():
    if __td_api is None:
        raise ModuleNotFoundError(
            "Unable to find TorsionDriveRecord which must be installed to use the TorsionDriveService"
        )


class TorsionDriveService(BaseService):

    # Index info
    service: str = "torsiondrive"
    program: str = "torsiondrive"
    procedure: str = "torsiondrive"

    # Program info
    optimization_program: str

    # Output
    output: TorsionDriveRecord = None  # added default

    # Temporaries
    torsiondrive_state: Dict[str, Any]
    optimization_history: Dict[str, List[str]] = {}

    # Task helpers
    task_map: Dict[str, List[str]] = {}
    task_manager: TaskManager = TaskManager()

    # Templates
    dihedral_template: str
    optimization_template: str
    molecule_template: str

    @classmethod
    def initialize_from_api(cls, storage_socket, logger, service_input, tag=None, priority=None):
        _check_td()
        import torsiondrive
        from torsiondrive import td_api

        # Build the record
        output = TorsionDriveRecord(
            **service_input.dict(exclude={"initial_molecule"}),
            initial_molecule=[x.id for x in service_input.initial_molecule],
            provenance={
                "creator": "torsiondrive",
                "version": torsiondrive.__version__,
                "routine": "torsiondrive.td_api",
            },
            final_energy_dict={},
            minimum_positions={},
            optimization_history={},
        )

        meta = {"output": output}

        # Remove identity info from molecule template
        molecule_template = copy.deepcopy(service_input.initial_molecule[0].dict(encoding="json"))
        molecule_template.pop("id", None)
        molecule_template.pop("identifiers", None)
        meta["molecule_template"] = json.dumps(molecule_template)

        # Initiate torsiondrive meta
        # The torsiondrive package uses print, so capture that using
        # contextlib
        td_stdout = io.StringIO()
        with contextlib.redirect_stdout(td_stdout):
            meta["torsiondrive_state"] = td_api.create_initial_state(
                dihedrals=output.keywords.dihedrals,
                grid_spacing=output.keywords.grid_spacing,
                elements=molecule_template["symbols"],
                init_coords=[x.geometry for x in service_input.initial_molecule],
                dihedral_ranges=output.keywords.dihedral_ranges,
                energy_decrease_thresh=output.keywords.energy_decrease_thresh,
                energy_upper_limit=output.keywords.energy_upper_limit,
            )

        meta["stdout"] = td_stdout.getvalue()

        # Build dihedral template
        dihedral_template = []
        for idx in output.keywords.dihedrals:
            tmp = {"type": "dihedral", "indices": idx}
            dihedral_template.append(tmp)

        meta["dihedral_template"] = json.dumps(dihedral_template)

        # Build optimization template
        opt_template = {
            "meta": {"procedure": "optimization", "qc_spec": output.qc_spec.dict(), "tag": meta.pop("tag", None)}
        }
        opt_template["meta"].update(output.optimization_spec.dict())
        meta["optimization_template"] = json.dumps(opt_template)

        # Move around geometric data
        meta["optimization_program"] = output.optimization_spec.program

        meta["hash_index"] = output.get_hash_index()

        meta["task_tag"] = tag
        meta["task_priority"] = priority
        return cls(**meta, storage_socket=storage_socket, logger=logger)

    def iterate(self):
        _check_td()
        from torsiondrive import td_api

        self.status = "RUNNING"

        # Check if tasks are done
        if self.task_manager.done() is False:
            return False

        complete_tasks = self.task_manager.get_tasks()

        # Populate task results
        task_results = {}
        for key, task_ids in self.task_map.items():
            task_results[key] = []

            for task_id in task_ids:
                # Cycle through all tasks for this entry
                ret = complete_tasks[task_id]

                # Lookup molecules
                initial_id = ret["initial_molecule"]
                final_id = ret["final_molecule"]

                mol_ids = [initial_id, final_id]
                mol_data = self.storage_socket.get_molecules(id=mol_ids)["data"]
                mol_map = {x.id: x.geometry for x in mol_data}

                task_results[key].append((mol_map[initial_id], mol_map[final_id], ret["energies"][-1]))

        # The torsiondrive package uses print, so capture that using
        # contextlib
        td_stdout = io.StringIO()
        with contextlib.redirect_stdout(td_stdout):
            td_api.update_state(self.torsiondrive_state, task_results)

            # Create new tasks from the current state
            next_tasks = td_api.next_jobs_from_state(self.torsiondrive_state, verbose=True)

        self.stdout += "\n" + td_stdout.getvalue()

        # All done
        if len(next_tasks) == 0:
            self.status = "COMPLETE"
            self.update_output()
            return True

        self.submit_optimization_tasks(next_tasks)

        return False

    def submit_optimization_tasks(self, task_dict):
        _check_td()
        from torsiondrive import td_api

        new_tasks = {}
        task_map = {}

        for key, geoms in task_dict.items():
            task_map[key] = []
            for num, geom in enumerate(geoms):

                # Update molecule
                packet = json.loads(self.optimization_template)

                # Construct constraints
                constraints = json.loads(self.dihedral_template)
                grid_id = td_api.grid_id_from_string(key)
                for con_num, k in enumerate(grid_id):
                    constraints[con_num]["value"] = k
                # update existing constraints to support the "extra constraints" feature
                packet["meta"]["keywords"].setdefault("constraints", {})
                packet["meta"]["keywords"]["constraints"].setdefault("set", [])
                packet["meta"]["keywords"]["constraints"]["set"].extend(constraints)

                # Build new molecule
                mol = json.loads(self.molecule_template)
                mol["geometry"] = geom
                packet["data"] = [mol]

                task_key = "{}-{}".format(key, num)
                new_tasks[task_key] = packet

                task_map[key].append(task_key)

        self.task_manager.submit_tasks("optimization", new_tasks)
        self.task_map = task_map

        # Update history
        for key, task_ids in self.task_map.items():
            if key not in self.optimization_history:
                self.optimization_history[key] = []

            for task_id in task_ids:
                self.optimization_history[key].append(self.task_manager.required_tasks[task_id])

        self.update_output()

    def update_output(self):
        """
        Adds data to the TorsionDriveRecord object
        """
        _check_td()
        from torsiondrive import td_api

        # # Get lowest energies and positions
        min_positions = {}
        final_energy = {}
        for k, v in self.torsiondrive_state["grid_status"].items():
            idx = int(np.argmin([x[2] for x in v]))
            key = json.dumps(td_api.grid_id_from_string(k))
            min_positions[key] = idx
            final_energy[key] = v[idx][2]

        history = {json.dumps(td_api.grid_id_from_string(k)): v for k, v in self.optimization_history.items()}

        self.output = self.output.copy(
            update={
                "status": self.status,
                "minimum_positions": min_positions,
                "final_energy_dict": final_energy,
                "optimization_history": history,
            }
        )

        return True
