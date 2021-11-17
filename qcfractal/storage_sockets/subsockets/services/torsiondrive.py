"""
Procedure for a torsiondrive service
"""
from __future__ import annotations

import io
import json
import logging
import contextlib
from importlib.util import find_spec
from datetime import datetime

import sqlalchemy.orm.attributes
from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload, load_only, selectinload
from ..procedures import helpers

__td_spec = find_spec("torsiondrive")
__td_api_spec = find_spec("torsiondrive.td_api")
if __td_spec is not None:
    torsiondrive = __td_spec.loader.load_module()
    td_api = __td_api_spec.loader.load_module()


def _check_td():
    if __td_spec is None:
        raise ModuleNotFoundError(
            "Unable to find the torsiondrive package, which must be installed to use the torsion drive service"
        )


from .base import BaseServiceHandler
from ...models import ServiceQueueORM, TorsionDriveProcedureORM, MoleculeORM, OptimizationHistory
from ...sqlalchemy_common import insert_general, get_query_proj_columns
from ....interface.models import (
    ObjectId,
    PriorityEnum,
    TorsionDriveInput,
    TorsionDriveRecord,
    Molecule,
    RecordStatusEnum,
    OptimizationProcedureSpecification,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session
    from ...sqlalchemy_socket import SQLAlchemySocket
    from ....interface.models import InsertMetadata
    from typing import List, Tuple, Sequence, Dict, Optional, Any

    TorsionDriveProcedureDict = Dict[str, Any]


class TorsionDriveHandler(BaseServiceHandler):
    """A handler for torsiondrive services"""

    def __init__(self, core_socket: SQLAlchemySocket):
        self._core_socket = core_socket
        self._logger = logging.getLogger(__name__)
        self._limit = core_socket.qcf_config.response_limits.record

        BaseServiceHandler.__init__(self, core_socket)

    def add_orm(
        self, torsiondrives: Sequence[TorsionDriveProcedureORM], *, session: Optional[Session] = None
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Adds TorsionDriveProcedureORM to the database, taking into account duplicates

        The session is flushed at the end of this function.

        Parameters
        ----------
        torsiondrives
            ORM objects to add to the database
        session
            An existing SQLAlchemy session to use. If None, one will be created. If an existing session
            is used, it will be flushed before returning from this function.

        Returns
        -------
        :
            Metadata showing what was added, and a list of returned torsiondrive ids. These will be in the
            same order as the inputs, and may correspond to newly-inserted ORMs or to existing data.
        """

        # TODO - HACK
        # need to get the hash (for now)
        for td in torsiondrives:
            r = TorsionDriveRecord(
                initial_molecule=[x.id for x in td.initial_molecule_obj],
                keywords=td.keywords,
                optimization_spec=td.optimization_spec,
                qc_spec=td.qc_spec,
                final_energy_dict={},
                optimization_history={},
                minimum_positions={},
            )
            td.hash_index = r.get_hash_index()

        with self._core_socket.optional_session(session) as session:
            meta, orm = insert_general(
                session, torsiondrives, (TorsionDriveProcedureORM.hash_index,), (TorsionDriveProcedureORM.id,)
            )
        return meta, [x[0] for x in orm]

    def get(
        self,
        id: Sequence[ObjectId],
        include: Optional[Sequence[str]] = None,
        exclude: Optional[Sequence[str]] = None,
        missing_ok: bool = False,
        *,
        session: Optional[Session] = None,
    ) -> List[Optional[TorsionDriveProcedureDict]]:
        """
        Obtain torsion drive procedure information from the database

        The returned information will be in order of the given ids

        If missing_ok is False, then any ids that are missing in the database will raise an exception. Otherwise,
        the corresponding entry in the returned list of results will be None.

        Parameters
        ----------
        session
            An existing SQLAlchemy session to get data from
        id
            A list or other sequence of result IDs
        include
            Which fields of the result to return. Default is to return all fields.
        exclude
            Remove these fields from the return. Default is to return all fields.
        missing_ok
           If set to True, then missing results will be tolerated, and the returned list of
           Molecules will contain None for the corresponding IDs that were not found.
        session
            An existing SQLAlchemy session to use. If None, one will be created

        Returns
        -------
        :
            Torsiondrive information as a dictionary in the same order as the given ids.
            If missing_ok is True, then this list will contain None where the molecule was missing.
        """

        if len(id) > self._limit:
            raise RuntimeError(f"Request for {len(id)} single results is over the limit of {self._limit}")

        # TODO - int id
        int_id = [int(x) for x in id]
        unique_ids = list(set(int_id))

        load_cols, load_rels = get_query_proj_columns(TorsionDriveProcedureORM, include, exclude)

        with self._core_socket.optional_session(session, True) as session:
            query = (
                session.query(TorsionDriveProcedureORM)
                .filter(TorsionDriveProcedureORM.id.in_(unique_ids))
                .options(load_only(*load_cols))
            )

            for r in load_rels:
                query = query.options(selectinload(r))

            results = query.yield_per(100)
            result_map = {r.id: r.dict() for r in results}

            # Put into the requested order
            ret = [result_map.get(x, None) for x in int_id]

            if missing_ok is False and None in ret:
                raise RuntimeError("Could not find all requested optimization records")

            return ret

    def verify_input(self, data):
        pass

    def create_records(
        self, session: Session, service_input: TorsionDriveInput
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Creates a torsiondrive procedure and its associated service

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        service_input
            Input to a torsiondrive calculation

        Returns
        -------
        :
            Metadata about which torsiondrive inputs are duplicates or newly-added, and a list of
            IDs corresponding to all the given torsiondrive procedures (both existing and newly-added).
            This will be in the same order as the inputs

        """
        meta, mol_ids = self._core_socket.molecule.add_mixed(service_input.initial_molecule)

        # TODO - int id
        mol_ids = [int(x) for x in mol_ids]

        initial_molecule_orm = session.query(MoleculeORM).filter(MoleculeORM.id.in_(mol_ids)).all()
        initial_molecule_orm = sorted(initial_molecule_orm, key=lambda x: mol_ids.index(x.id))

        if len(initial_molecule_orm) != len(mol_ids):
            raise RuntimeError("Cannot find all molecules for torsion drive?")

        td_orm = TorsionDriveProcedureORM()
        td_orm.keywords = service_input.keywords.dict()
        td_orm.optimization_spec = service_input.optimization_spec.dict()
        td_orm.qc_spec = service_input.qc_spec.dict()
        td_orm.initial_molecule_obj = initial_molecule_orm
        td_orm.provenance = {
            "creator": "torsiondrive",
            "version": torsiondrive.__version__,
            "routine": "torsiondrive.td_api",
        }

        td_orm.final_energy_dict = {}
        td_orm.minimum_positions = {}
        td_orm.protocols = {}
        td_orm.extras = {}

        # Add this ORM to the database, taking into account duplicates
        insert_meta, td_ids = self.add_orm([td_orm], session=session)

        return insert_meta, td_ids

    def create_services(
        self,
        session: Session,
        td_orms: Sequence[TorsionDriveProcedureORM],
        tag: Optional[str],
        priority: PriorityEnum,
    ) -> Tuple[InsertMetadata, List[ObjectId]]:
        """
        Creates services for torsiondrive procedures

        Parameters
        ----------
        session
            An existing SQLAlchemy session to use
        tag
            Tag associated with the new services (and all their tasks)
        priority
            The priority associated with the new services (and all their tasks)

        Returns
        -------
        :
            Metadata about which services are duplicates or newly-added, and a list of IDs corresponding to all
            the services for the given procedure IDs. If there was an error for a particular id,
            then the returned ID will be None.
        """

        new_services = []

        # Go over the input ids in order
        for idx, td_orm in enumerate(td_orms):
            service_state = {}

            # Create a template from the first initial molecule
            # We will assume all molecules only differ by geometry
            # TODO - check this
            initial_molecule = td_orm.initial_molecule_obj[0].dict()
            molecule_template = Molecule(**initial_molecule).dict(encoding="json")
            molecule_template.pop("id", None)
            molecule_template.pop("identifiers", None)
            service_state["molecule_template"] = json.dumps(molecule_template)

            # The torsiondrive package uses print, so capture that using contextlib
            td_stdout = io.StringIO()
            with contextlib.redirect_stdout(td_stdout):
                service_state["torsiondrive_state"] = td_api.create_initial_state(
                    dihedrals=td_orm.keywords["dihedrals"],
                    grid_spacing=td_orm.keywords["grid_spacing"],
                    elements=molecule_template["symbols"],
                    init_coords=[x.geometry.tolist() for x in td_orm.initial_molecule_obj],
                    dihedral_ranges=td_orm.keywords["dihedral_ranges"],
                    energy_decrease_thresh=td_orm.keywords["energy_decrease_thresh"],
                    energy_upper_limit=td_orm.keywords["energy_upper_limit"],
                )

            stdout = td_stdout.getvalue()

            # Build dihedral template
            dihedral_template = []
            for idx in td_orm.keywords["dihedrals"]:
                tmp = {"type": "dihedral", "indices": idx}
                dihedral_template.append(tmp)

            service_state["dihedral_template"] = json.dumps(dihedral_template)

            # Build optimization template
            opt_template = {
                "procedure": "optimization",
                "qc_spec": td_orm.qc_spec,
                "tag": tag,
                "priority": priority,
            }
            opt_template.update(td_orm.optimization_spec)
            service_state["optimization_template"] = json.dumps({"meta": opt_template})

            # Move around geometric data
            service_state["optimization_program"] = td_orm.optimization_spec["program"]

            # Now create the service ORM
            svc_orm = ServiceQueueORM()
            svc_orm.tag = tag
            svc_orm.priority = priority
            svc_orm.procedure_id = td_orm.id
            svc_orm.created_on = datetime.utcnow()
            svc_orm.modified_on = datetime.utcnow()
            svc_orm.service_state = service_state

            new_services.append(svc_orm)

            # Add the output to the base procedure
            td_orm.stdout = self._core_socket.output_store.add([stdout])[0]

        return self._core_socket.service_queue.add_orm(new_services, session=session)

    def iterate(self, session: Session, td_service_orm: ServiceQueueORM) -> bool:
        if td_service_orm.procedure_obj.status not in [RecordStatusEnum.running, RecordStatusEnum.waiting]:
            # This is a programmer error
            raise RuntimeError(
                f"Torsion drive {td_service_orm.id}/Base result {td_service_orm.procedure_id} has status {td_service_orm.procedure_obj.status} - cannot iterate with that status!"
            )

        # Is this the first iteration?
        if td_service_orm.procedure_obj.status == RecordStatusEnum.waiting:
            td_service_orm.procedure_obj.status = RecordStatusEnum.running

        # Load the state from the service_state column
        td_service_state = td_service_orm.service_state

        # Check if tasks are done (should be checked already)
        assert self._core_socket.service.tasks_done(td_service_orm) == (True, True)

        # Sort by position
        # Fully sorting by the key is not important since that ends up being a key in the dict
        # All that matters is that position 1 for a particular key comes before position 2, etc
        complete_tasks = sorted(td_service_orm.tasks_obj, key=lambda x: x.extras["position"])

        # Populate task results needed by the torsiondrive package
        task_results = {}
        for task in complete_tasks:
            td_api_key = task.extras["td_api_key"]
            task_results.setdefault(td_api_key, [])

            # This is an ORM for an optimization
            proc_obj = task.procedure_obj

            # Lookup molecules
            initial_id = proc_obj.initial_molecule
            final_id = proc_obj.final_molecule
            mol_ids = [initial_id, final_id]
            mol_data = self._core_socket.molecule.get(id=mol_ids, include=["geometry"], session=session)

            initial_mol_geom = mol_data[0]["geometry"].tolist()
            final_mol_geom = mol_data[1]["geometry"].tolist()
            task_results[td_api_key].append((initial_mol_geom, final_mol_geom, proc_obj.energies[-1]))

        # The torsiondrive package uses print, so capture that using contextlib
        td_stdout = io.StringIO()
        with contextlib.redirect_stdout(td_stdout):
            td_api.update_state(td_service_state["torsiondrive_state"], task_results)
            next_tasks = td_api.next_jobs_from_state(td_service_state["torsiondrive_state"], verbose=True)

        stdout_append = "\n" + td_stdout.getvalue()

        # If no tasks are left, we are all done
        if len(next_tasks) == 0:
            td_service_orm.procedure_obj.status = RecordStatusEnum.complete
        else:
            self.submit_optimization_tasks(session, td_service_state, td_service_orm, next_tasks)

        # Update the torsiondrive procedure itself
        min_positions = {}
        final_energy = {}
        for k, v in td_service_state["torsiondrive_state"]["grid_status"].items():
            energies = [x[2] for x in v]
            idx = energies.index(min(energies))
            key = json.dumps(td_api.grid_id_from_string(k))
            min_positions[key] = idx
            final_energy[key] = energies[idx]

        td_service_orm.procedure_obj.minimum_positions = min_positions
        td_service_orm.procedure_obj.final_energy_dict = final_energy

        td_service_orm.procedure_obj.stdout = self._core_socket.output_store.append(
            td_service_orm.procedure_obj.stdout, stdout_append, session=session
        )

        # Set the new service state. We must then mark it as modified
        # so that SQLAlchemy can pick up changes. This is because SQLAlchemy
        # cannot track mutations in nested dicts
        td_service_orm.service_state = td_service_state
        sqlalchemy.orm.attributes.flag_modified(td_service_orm, "service_state")

        # Return True to indicate that this service has successfully completed
        return len(next_tasks) == 0

    def submit_optimization_tasks(
        self, session: Session, td_service_state: Dict[str, Any], td_service_orm: ServiceQueueORM, task_dict
    ):
        new_tasks = []

        for td_api_key, geometries in task_dict.items():
            for position, geometry in enumerate(geometries):

                # Create an optimization input based on the new geometry and the optimization template
                new_opt = json.loads(td_service_state["optimization_template"])

                # Construct constraints
                constraints = json.loads(td_service_state["dihedral_template"])
                grid_id = td_api.grid_id_from_string(td_api_key)
                for con_num, k in enumerate(grid_id):
                    constraints[con_num]["value"] = k

                # update the constraints
                new_opt["meta"]["keywords"].setdefault("constraints", {})
                new_opt["meta"]["keywords"]["constraints"].setdefault("set", [])
                new_opt["meta"]["keywords"]["constraints"]["set"].extend(constraints)

                # Build new molecule
                mol = json.loads(td_service_state["molecule_template"])
                mol["geometry"] = geometry
                new_opt["data"] = mol

                new_tasks.append(
                    (
                        {"td_api_key": td_api_key, "position": position},
                        Molecule(**new_opt["data"]),
                        OptimizationProcedureSpecification(**new_opt["meta"]),
                    )
                )

        added_ids = self.submit_tasks(session, td_service_orm, new_tasks)

        # Update history
        for id, (task_info, _, _) in zip(added_ids, new_tasks):
            td_api_key = task_info["td_api_key"]
            opt_key = json.dumps(td_api.grid_id_from_string(td_api_key))

            opt_history = OptimizationHistory(torsion_id=int(td_service_orm.procedure_obj.id), opt_id=id, key=opt_key)
            td_service_orm.procedure_obj.optimization_history_obj.append(opt_history)

        # Add positions to the association table
        # I don't always trust databases to ensure the order is always correct
        for idx, obj in enumerate(td_service_orm.procedure_obj.optimization_history_obj):
            obj.position = idx
