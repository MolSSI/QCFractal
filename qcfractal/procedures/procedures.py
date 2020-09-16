"""
All procedures tasks involved in on-node computation.
"""

import abc
from typing import List, Union, Dict, Any, Optional

import qcelemental as qcel

import qcengine as qcng

from ..interface.models import Molecule, OptimizationRecord, QCSpecification, ResultRecord, TaskRecord, KVStore, KeywordSet
from ..interface.models.task_models import PriorityEnum
from .procedures_util import parse_single_tasks, form_qcinputspec_schema

_wfn_return_names = set(qcel.models.results.WavefunctionProperties._return_results_names)
_wfn_all_fields = set(qcel.models.results.WavefunctionProperties.__fields__.keys())


class BaseTasks(abc.ABC):
    def __init__(self, storage, logger):
        self.storage = storage
        self.logger = logger

    def submit_tasks(self, data):
        """
        Creates results/procedures and tasks in the database
        """

        results_ids, existing_ids = self.parse_input(data)
        submitted_ids = [x for x in results_ids if x not in existing_ids and x is not None]

        n_inserted = 0
        missing = []
        for num, x in enumerate(results_ids):
            if x is None:
                missing.append(num)
            else:
                n_inserted += 1

        results = {
            "meta": {
                "n_inserted": n_inserted,
                "duplicates": [],
                "validation_errors": [],
                "success": True,
                "error_description": False,
                "errors": [],
            },
            "data": {"ids": results_ids, "submitted": submitted_ids, "existing": existing_ids},
        }

        return results

    def retrieve_outputs(self, rdata):
        """
        Retrieves (possibly compressed) outputs from an AtomicResult (that has been converted to a dictionary)

        This function modifies the rdata dictionary in-place
        """

        # Get the compressed outputs if they exist
        stdout = rdata["extras"].pop("_qcfractal_compressed_stdout", None)
        stderr = rdata["extras"].pop("_qcfractal_compressed_stderr", None)
        error = rdata["extras"].pop("_qcfractal_compressed_error", None)

        # Create KVStore objects from these
        if stdout is not None:
            stdout = KVStore(**stdout)
        if stderr is not None:
            stderr = KVStore(**stderr)
        if error is not None:
            error = KVStore(**error)

        # This shouldn't happen, but if they aren't compressed, check for
        # uncompressed
        if stdout is None and rdata.get("stdout", None) is not None:
            self.logger.warning(f"Found uncompressed stdout for result id {rdata['id']}")
            stdout = KVStore(data=rdata["stdout"])
        if stderr is None and rdata.get("stderr", None) is not None:
            self.logger.warning(f"Found uncompressed stderr for result id {rdata['id']}")
            stderr = KVStore(data=rdata["stderr"])
        if error is None and rdata.get("error", None) is not None:
            self.logger.warning(f"Found uncompressed error for result id {rdata['id']}")
            error = KVStore(data=rdata["error"])

        # Now add to the database and set the ids in the diction
        outputs = [stdout, stderr, error]
        stdout_id, stderr_id, error_id = self.storage.add_kvstore(outputs)["data"]
        rdata["stdout"] = stdout_id
        rdata["stderr"] = stderr_id
        rdata["error"] = error_id

    @abc.abstractmethod
    def verify_input(self, data):
        pass

    @abc.abstractmethod
    def parse_input(self, data):
        pass

    @abc.abstractmethod
    def handle_completed_output(self, data):
        pass


class SingleResultTasks(BaseTasks):
    """A task generator for a single QC computation task.

    This is a single quantum calculation, unique by program, driver, method, basis, keywords, molecule.
    """

    def verify_input(self, data):
        program = data.meta.program.lower()
        if program not in qcng.list_all_programs():
            return f"Program '{program}' not available in QCEngine."

        if data.meta.dict().get("protocols", None) is not None:
            try:
                qcel.models.results.ResultProtocols(**data.meta.protocols)
            except Exception as e:
                return f"Could not validate protocols: {str(e)}"

        return True

    def parse_input(self, data):
        """Parse input json into internally appropriate format


        Format of the input data:
        data = {
            "meta": {
                "procedure": "single",
                "driver": "energy",
                "method": "HF",
                "basis": "sto-3g",
                "keywords": "default",
                "program": "psi4"
                },
            },
            "data": ["mol_id_1", "mol_id_2", ...],
        }

        """

        # Get the qcspec from the input metadata dictionary
        qc_spec_dict = data.meta.dict()

        # We should only have gotten here if procedure is 'single'
        procedure = qc_spec_dict.pop("procedure")
        assert procedure.lower() == 'single'

        # Grab the tag and priority if available
        # These are not used in the ResultRecord, so we can pop them
        tag = qc_spec_dict.pop("tag")
        priority = qc_spec_dict.pop("priority")

        # Handle keywords, which may be None
        if data.meta.keywords is not None:
            # QCSpec will only hold the ID
            keywords = self.storage.get_add_keywords_mixed([data.meta.keywords])["data"][0]
            if keywords is None:
                raise KeyError("Could not find requested KeywordsSet from id key.")
            qc_spec_dict["keywords"] = keywords.id
        else:
            keywords = None
            qc_spec_dict["keywords"] = None

        # Will also validate the qc_spec
        # TODO - do this after migrating results to QCSpecification?
        # qc_spec = QCSpecification(**qc_spec_dict)

        # Add all the molecules to the database
        # TODO: WARNING WARNING if get_add_molecules_mixed is modified to handle duplicates
        #       correctly, you must change some pieces later in this function
        input_molecules = data.data
        molecule_list = self.storage.get_add_molecules_mixed(input_molecules)["data"]

        # Keep molecule IDs that are not None
        # Molecule IDs may be None if they are duplicates (ie, the same molecule was listed twice
        # in data.data) or an id specified in data.data was invalid
        valid_molecule_idx = [idx for idx, mol in enumerate(molecule_list) if mol is not None]
        valid_molecules = [x for x in molecule_list if x is not None]

        # Create ResultRecords for everything
        all_result_records = []
        for mol in valid_molecules:
            record = ResultRecord(**qc_spec_dict.copy(), molecule=mol.id)
            all_result_records.append(record)

        # Add all results in a single function call
        # NOTE: Because get_add_molecules_mixed returns None for duplicate
        # molecules (or when specifying incorrect ids),
        # all_result_records should never contain duplicates
        ret = self.storage.add_results(all_result_records)

        # Get all the result IDs (may be new or existing)
        # These will be in the order we sent to add_results
        all_result_ids = ret["data"]
        existing_ids = ret["meta"]["duplicates"]

        # Assign ids to the result records
        for idx in range(len(all_result_records)):
            r = all_result_records[idx].copy(update={'id': all_result_ids[idx]})
            all_result_records[idx] = r

        # Now generate all the tasks, but only for results that don't exist already
        new_task_records = [x for x in all_result_records if x.id not in existing_ids]
        #self.create_tasks(all_result_records, valid_molecules, [keywords]*len(all_result_records), tag=tag, priority=priority)
        self.create_tasks(all_result_records, tag=tag, priority=priority)

        # Keep the returned result id list in the same order as the input molecule list
        # If a molecule was None, then the corresponding result ID will be None
        # (since the entry in valid_molecule_idx will be missing). Ditto for molecules specified
        # more than once in the argument to this function
        results_ids = [None] *  len(molecule_list)
        for idx, result_id in zip(valid_molecule_idx, all_result_ids):
            results_ids[idx] = result_id

        return results_ids, existing_ids

    def create_tasks(self, records: List[ResultRecord], molecules: Optional[List[Molecule]] = None, keywords: Optional[List[KeywordSet]] = None,
                    tag: Optional[str] = None, priority: Optional[PriorityEnum] = None):
        """
        Creates TaskRecord objects based on a record and molecules/keywords

        If molecules are not given, then they are loaded from the database (with the id given in the result record)
        The same applies to the keywords.

        Parameters
        ----------
        records: List[ResultRecord]
            Records for which to create the TaskRecord object
        molecules: Optional[List[Molecule]]
            Molecules to be applied to the records. If given, must be the same length as records, and
            must be in the same order. They must have been added to the database already and have an id.
        keywords: Optional[KeywordSet]
            QC Keywords to use in the calculation. If given, must be the same length as records, and
            be in the same order. They must have been added to the database already and have an id.

        Returns
        -------
        List[TaskRecord]
            TaskRecords that can be added to the database
        """

        # Find the molecule keywords specified in the records
        rec_mol_ids = [x.molecule for x in records]

        # If not specified when calling this function, load them from the database
        # TODO: there can be issues with duplicate molecules. So we have to go one by one
        if molecules is None:
            molecules = [self.storage.get_molecules(x)["data"][0] for x in rec_mol_ids]

        # Check id to make sure the molecules match the ids in the records
        mol_ids = [x.id for x in molecules]
        if rec_mol_ids != mol_ids:
            raise ValueError(f"Given molecule ids {str(mol_ids)} do not match those in records: {str(rec_mol_ids)}")

        # Do the same as above but with with keywords
        rec_kw_ids = [x.keywords for x in records]
        if keywords is None:
            keywords = [self.storage.get_keywords(x)["data"][0] if x is not None else None for x in rec_kw_ids]

        kw_ids = [x.id if x is not None else None for x in keywords]
        if rec_kw_ids != kw_ids:
            raise ValueError(f"Given keyword ids {str(kw_ids)} do not match those in records: {str(rec_kw_ids)}")

        # Create QCSchema inputs and tasks for everything, too
        new_tasks = []

        for rec, mol, kw in zip(records, molecules, keywords):
            inp = self._build_schema_input(rec, mol, kw)
            inp.extras["_qcfractal_tags"] = {"program": rec.program, "keywords": rec.keywords}

            # Build task object
            task = TaskRecord(
                **{
                    "spec": {
                        "function": "qcengine.compute",  # todo: add defaults in models
                        "args": [inp.dict(), rec.program],
                        "kwargs": {},  # todo: add defaults in models
                    },
                    "parser": "single",
                    "program": rec.program,
                    "tag": tag,
                    "priority": priority,
                    "base_result": rec.id,
                }
            )

            new_tasks.append(task)

        self.storage.queue_submit(new_tasks)


    def handle_completed_output(self, result_outputs):

        completed_tasks = []
        updates = []

        for output in result_outputs:
            # Find the existing result information in the database
            base_id = output["base_result"]
            existing_result = self.storage.get_results(id=base_id)
            if existing_result["meta"]["n_found"] != 1:
                raise KeyError(f"Could not find existing base result {base_id}")

            # Get the original result data from the dictionary
            existing_result = existing_result["data"][0]

            # Some consistency checks:
            # Is this marked as incomplete?
            # TODO: Check manager, although that information isn't sent to us right now
            if existing_result["status"] != "INCOMPLETE":
                self.logger.warning(f"Skipping returned results for base_id={base_id}, as it is not marked incomplete")
                continue

            rdata = output["result"]

            # Adds the results to the database and sets the appropriate fields
            # inside the dictionary
            self.retrieve_outputs(rdata)

            # Store Wavefunction data
            if rdata.get("wavefunction", False):
                wfn = rdata.get("wavefunction", False)
                available = set(wfn.keys()) - {"restricted", "basis"}
                return_map = {k: wfn[k] for k in wfn.keys() & _wfn_return_names}

                rdata["wavefunction"] = {
                    "available": list(available),
                    "restricted": wfn["restricted"],
                    "return_map": return_map,
                }

                # Extra fields are trimmed as we have a column *per* wavefunction structure.
                available_keys = wfn.keys() - _wfn_return_names
                if available_keys > _wfn_all_fields:
                    self.logger.warning(
                        f"Too much wavefunction data for result {base_id}, removing extra data."
                    )
                    available_keys &= _wfn_all_fields

                wavefunction_save = {k: wfn[k] for k in available_keys}
                wfn_data_id = self.storage.add_wavefunction_store([wavefunction_save])["data"][0]
                rdata["wavefunction_data_id"] = wfn_data_id

            # Create an updated ResultRecord based on the existing record and the new results
            # Double check to make sure everything is consistent
            assert existing_result["method"] == rdata["model"]["method"]
            assert existing_result["basis"] == rdata["model"]["basis"]
            assert existing_result["driver"] == rdata["driver"]
            assert existing_result["molecule"] == rdata["molecule"]['id']

            # Result specific
            existing_result["extras"] = rdata["extras"]
            existing_result["return_result"] = rdata["return_result"]
            existing_result["properties"] = rdata["properties"]

            # Wavefunction data
            existing_result["wavefunction"] = rdata.get("wavefunction", None)
            existing_result["wavefunction_data_id"] = rdata.get("wavefunction_data_id", None)

            # Standard blocks
            existing_result["provenance"] = rdata["provenance"]
            existing_result["error"] = rdata["error"]
            existing_result["stdout"] = rdata["stdout"]
            existing_result["stderr"] = rdata["stderr"]
            existing_result["status"] = "COMPLETE"

            result = ResultRecord(**existing_result)

            # Add to the list to be updated
            updates.append(result)
            completed_tasks.append(output["task_id"])

        self.storage.update_results(updates)

        return completed_tasks


    @staticmethod
    def _build_schema_input(
        record: ResultRecord, molecule: "Molecule", keywords: Optional["KeywordSet"] = None) -> "ResultInput":
        """
        Creates an input schema for a single calculation
        """

        # Check for programmer sanity. Since we are building this input
        # right after creating the ResultRecord, these should never fail.
        # But would be very hard to debug
        assert record.molecule == molecule.id
        if record.keywords:
            assert record.keywords == keywords.id

        # Now start creating the "model" parameter for ResultInput
        model = {"method": record.method}

        if record.basis:
            model["basis"] = record.basis

        if not record.keywords:
            keywords = {}
        else:
            keywords = keywords.values

        if not record.protocols:
            protocols = {}
        else:
            protocols = record.protocols

        return qcel.models.AtomicInput(
            id=record.id,
            driver=record.driver.name,
            model=model,
            molecule=molecule,
            keywords=keywords,
            extras=record.extras,
            protocols=protocols,
        )


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
        assert opt_spec.procedure.lower() == 'optimization'

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
                "program": opt_spec.program
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
            r = all_opt_records[idx].copy(update={'id': all_opt_ids[idx]})
            all_opt_records[idx] = r

        # Now generate all the tasks, but only for results that don't exist already
        new_task_records = [x for x in all_opt_records if x.id not in existing_ids]
        #self.create_tasks(all_opt_records, valid_molecules, [qc_keywords]*len(all_opt_records), tag=tag, priority=priority)
        self.create_tasks(all_opt_records, tag=tag, priority=priority)

        # Keep the returned result id list in the same order as the input molecule list
        # If a molecule was None, then the corresponding result ID will be None
        # (since the entry in valid_molecule_idx will be missing). Ditto for molecules specified
        # more than once in the argument to this function
        opt_ids = [None] *  len(molecule_list)
        for idx, result_id in zip(valid_molecule_idx, all_opt_ids):
            opt_ids[idx] = result_id

        return opt_ids, existing_ids


    def create_tasks(self, records: List[OptimizationRecord], molecules: Optional[List[Molecule]] = None,
                     qc_keywords: Optional[List[KeywordSet]] = None,
                     tag: Optional[str] = None, priority: Optional[PriorityEnum] = None):


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
            inp.input_specification.extras["_qcfractal_tags"] = {
                "program": rec.qc_spec.program,
                "keywords": rec.qc_spec.keywords, # Just the id?
            }

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
                    "base_result": rec.id
                }
            )

            new_tasks.append(task)

        self.storage.queue_submit(new_tasks)

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

            results = parse_single_tasks(self.storage, traj_dict)
            for k, v in results.items():
                v["task_id"] = output["task_id"]
                results[k] = ResultRecord(**v)

            ret = self.storage.add_results(list(results.values()))
            update_dict["trajectory"] = ret["data"]
            update_dict["energies"] = procedure["energies"]
            update_dict["provenance"] = procedure["provenance"]

            rec = OptimizationRecord(**{**rec.dict(), **update_dict})
            updates.append(rec)
            completed_tasks.append(output["task_id"])

        self.storage.update_procedures(updates)

        return completed_tasks

    @staticmethod
    def _build_schema_input(
            record : OptimizationRecord, initial_molecule: "Molecule", qc_keywords: Optional["KeywordSet"] = None
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



# ----------------------------------------------------------------------------

supported_procedures = Union[SingleResultTasks, OptimizationTasks]
__procedure_map = {"single": SingleResultTasks, "optimization": OptimizationTasks}


def check_procedure_available(procedure: str) -> List[str]:
    """
    Lists all available procedures
    """
    return procedure.lower() in __procedure_map


def get_procedure_parser(procedure_type: str, storage, logger) -> supported_procedures:
    """A factory method that returns the appropriate parser class
    for the supported procedure types (like single and optimization)

    Parameters
    ---------
    procedure_type: str, 'single' or 'optimization'
    storage: storage socket object
        such as MongoengineSocket object

    Returns
    -------
    A parser class corresponding to the procedure_type:
        'single' --> SingleResultTasks
        'optimization' --> OptimizationTasks
    """

    try:
        return __procedure_map[procedure_type.lower()](storage, logger)
    except KeyError:
        raise KeyError("Procedure type ({}) is not suported yet.".format(procedure_type))
