"""
Optimization procedure/task
"""

from typing import List, Optional

import qcelemental as qcel
import qcengine as qcng

from .base import BaseTasks
from ..interface.models import Molecule, OptimizationRecord, QCSpecification, ResultRecord, TaskRecord, KeywordSet
from ..interface.models.task_models import PriorityEnum
from .procedures_util import parse_single_tasks, form_qcinputspec_schema


class OptimizationTasks(BaseTasks):
    """
    Optimization task manipulation
    """

    def verify_input(self, data):
        program = data.meta.program.lower()
        if program not in qcng.list_all_procedures():
            return "Procedure '{}' not available in QCEngine.".format(program)

        program = data.meta.qc_spec["program"].lower()
        if program not in qcng.list_all_programs():
            return "Program '{}' not available in QCEngine.".format(program)

        return True

    def parse_input(self, data, duplicate_id="hash_index"):
        """Parse input json into internally appropriate format

        json_data = {
            "meta": {
                "procedure": "optimization",
                "option": "default",
                "program": "geometric",
                "qc_meta": {
                    "driver": "energy",
                    "method": "HF",
                    "basis": "sto-3g",
                    "keywords": "default",
                    "program": "psi4"
                },
            },
            "data": ["mol_id_1", "mol_id_2", ...],
        }

        qc_schema_input = {
            "molecule": {
                "geometry": [
                    0.0,  0.0, -0.6,
                    0.0,  0.0,  0.6,
                ],
                "symbols": ["H", "H"],
                "connectivity": [[0, 1, 1]]
            },
            "driver": "gradient",
            "model": {
                "method": "HF",
                "basis": "sto-3g"
            },
            "keywords": {},
        }
        json_data = {
            "keywords": {
                "coordsys": "tric",
                "maxiter": 100,
                "program": "psi4"
            },
        }

        """

        # Get the optimization specification from the input meta dictionary
        opt_spec = data.meta

        # We should only have gotten here if procedure is 'optimization'
        assert opt_spec.procedure.lower() == "optimization"

        # Grab the tag and priority if available
        tag = opt_spec.tag
        priority = opt_spec.priority

        # Handle (optimization) keywords, which may be None
        # TODO: These are not stored in the keywords table (yet)
        opt_keywords = {} if opt_spec.keywords is None else opt_spec.keywords

        # Set the program used for gradient evaluations. This is stored in the input qcspec
        # but the QCInputSpecification does not have a place for program. So instead
        # we move it to the optimization keywords
        opt_keywords["program"] = opt_spec.qc_spec["program"]

        # Pull out the QCSpecification from the input
        qc_spec_dict = data.meta.qc_spec

        # Handle qc specification keywords, which may be None
        qc_keywords = qc_spec_dict.get("keywords", None)
        if qc_keywords is not None:
            # The keywords passed in may contain the entire KeywordSet.
            # But the QCSpec will only hold the ID
            qc_keywords = self.storage.get_add_keywords_mixed([qc_keywords])["data"][0]
            if qc_keywords is None:
                raise KeyError("Could not find requested KeywordsSet from id key.")
            qc_spec_dict["keywords"] = qc_keywords.id

        # Now that keywords are fixed we can do this
        qc_spec = QCSpecification(**qc_spec_dict)

        # Add all the initial molecules to the database
        # TODO: WARNING WARNING if get_add_molecules_mixed is modified to handle duplicates
        #       correctly, you must change some pieces later in this function
        molecule_list = self.storage.get_add_molecules_mixed(data.data)["data"]

        # Keep molecule IDs that are not None
        # Molecule IDs may be None if they are duplicates (ie, the same molecule was listed twice
        # in data.data) or an id specified in data.data was invalid
        valid_molecule_idx = [idx for idx, mol in enumerate(molecule_list) if mol is not None]
        valid_molecules = [x for x in molecule_list if x is not None]

        # Create all OptimizationRecords
        all_opt_records = []
        for mol in valid_molecules:
            # TODO fix handling of protocols (perhaps after hardening rest models)
            opt_data = {
                "initial_molecule": mol.id,
                "qc_spec": qc_spec,
                "keywords": opt_keywords,
                "program": opt_spec.program,
            }
            if hasattr(opt_spec, "protocols"):
                opt_data["protocols"] = data.meta.protocols

            opt_rec = OptimizationRecord(**opt_data)
            all_opt_records.append(opt_rec)

        # Add all the procedures in a single function call
        # NOTE: Because get_add_molecules_mixed returns None for duplicate
        # molecules (or when specifying incorrect ids),
        # all_opt_records should never contain duplicates
        ret = self.storage.add_procedures(all_opt_records)

        # Get all procedure IDs (may be new or existing)
        # These will be in the order we sent to add_results
        all_opt_ids = ret["data"]
        existing_ids = ret["meta"]["duplicates"]

        # Assing ids to the optimization records
        for idx in range(len(all_opt_records)):
            r = all_opt_records[idx].copy(update={"id": all_opt_ids[idx]})
            all_opt_records[idx] = r

        # Now generate all the tasks, but only for results that don't exist already
        self.create_tasks(
            all_opt_records, valid_molecules, [qc_keywords] * len(all_opt_records), tag=tag, priority=priority
        )

        # Keep the returned result id list in the same order as the input molecule list
        # If a molecule was None, then the corresponding result ID will be None
        # (since the entry in valid_molecule_idx will be missing). Ditto for molecules specified
        # more than once in the argument to this function
        opt_ids = [None] * len(molecule_list)
        for idx, result_id in zip(valid_molecule_idx, all_opt_ids):
            opt_ids[idx] = result_id

        return opt_ids, existing_ids

    def create_tasks(
        self,
        records: List[OptimizationRecord],
        molecules: Optional[List[Molecule]] = None,
        qc_keywords: Optional[List[KeywordSet]] = None,
        tag: Optional[str] = None,
        priority: Optional[PriorityEnum] = None,
    ):

        # Find the molecule keywords specified in the records
        rec_mol_ids = [x.initial_molecule for x in records]

        # If not specified when calling this function, load them from the database
        # TODO: there can be issues with duplicate molecules. So we have to go one by one
        if molecules is None:
            molecules = [self.storage.get_molecules(x)["data"][0] for x in rec_mol_ids]

        # Check id to make sure the molecules match the ids in the records
        mol_ids = [x.id for x in molecules]
        if rec_mol_ids != mol_ids:
            raise ValueError(f"Given molecule ids {str(mol_ids)} do not match those in records: {str(rec_mol_ids)}")

        # Do the same as above but with with qc specification keywords
        rec_qc_kw_ids = [x.qc_spec.keywords for x in records]
        if qc_keywords is None:
            qc_keywords = [self.storage.get_keywords(x)["data"][0] if x is not None else None for x in rec_qc_kw_ids]

        qc_kw_ids = [x.id if x is not None else None for x in qc_keywords]
        if rec_qc_kw_ids != qc_kw_ids:
            raise ValueError(f"Given keyword ids {str(qc_kw_ids)} do not match those in records: {str(rec_qc_kw_ids)}")

        new_tasks = []
        for rec, mol, kw in zip(records, molecules, qc_keywords):
            inp = self._build_schema_input(rec, mol, kw)

            # Build task object
            task = TaskRecord(
                **{
                    "spec": {
                        "function": "qcengine.compute_procedure",
                        "args": [inp.dict(), rec.program],
                        "kwargs": {},
                    },
                    "parser": "optimization",
                    # TODO This is pretty whacked. Fix column names at some point
                    "program": rec.qc_spec.program,
                    "procedure": rec.program,
                    "tag": tag,
                    "priority": priority,
                    "base_result": rec.id,
                }
            )

            new_tasks.append(task)

        return self.storage.queue_submit(new_tasks)

    def handle_completed_output(self, opt_outputs):
        """Save the results of the procedure.
        It must make sure to save the results in the results table
        including the task_id in the TaskQueue table
        """

        completed_tasks = []
        updates = []
        for output in opt_outputs:
            rec = self.storage.get_procedures(id=output["base_result"])["data"][0]
            rec = OptimizationRecord(**rec)

            procedure = output["result"]

            # Adds the results to the database and sets the ids inside the dictionary
            self.retrieve_outputs(procedure)

            # Add initial and final molecules
            update_dict = {}
            update_dict["stdout"] = procedure.get("stdout", None)
            update_dict["stderr"] = procedure.get("stderr", None)
            update_dict["error"] = procedure.get("error", None)

            initial_mol, final_mol = self.storage.add_molecules(
                [Molecule(**procedure["initial_molecule"]), Molecule(**procedure["final_molecule"])]
            )["data"]
            assert initial_mol == rec.initial_molecule
            update_dict["final_molecule"] = final_mol

            # Parse trajectory computations and add task_id
            traj_dict = {k: v for k, v in enumerate(procedure["trajectory"])}

            # Add results for the trajectory to the database
            for k, v in traj_dict.items():
                self.retrieve_outputs(v)

            results = parse_single_tasks(self.storage, traj_dict, rec.qc_spec)
            for k, v in results.items():
                results[k] = ResultRecord(**v)

            ret = self.storage.add_results(list(results.values()))
            update_dict["trajectory"] = ret["data"]
            update_dict["energies"] = procedure["energies"]
            update_dict["provenance"] = procedure["provenance"]

            rec = OptimizationRecord(**{**rec.dict(), **update_dict})
            updates.append(rec)
            completed_tasks.append(output["task_id"])

        self.storage.update_procedures(updates)
        self.storage.queue_mark_complete(completed_tasks)

        return completed_tasks

    @staticmethod
    def _build_schema_input(
        record: OptimizationRecord, initial_molecule: "Molecule", qc_keywords: Optional["KeywordSet"] = None
    ) -> "OptimizationInput":
        """
        Creates a OptimizationInput schema.
        """

        assert record.initial_molecule == initial_molecule.id
        if record.qc_spec.keywords:
            assert record.qc_spec.keywords == qc_keywords.id

        qcinput_spec = form_qcinputspec_schema(record.qc_spec, keywords=qc_keywords)

        model = qcel.models.OptimizationInput(
            id=record.id,
            initial_molecule=initial_molecule,
            keywords=record.keywords,
            extras=record.extras,
            hash_index=record.hash_index,
            input_specification=qcinput_spec,
            protocols=record.protocols,
        )
        return model
