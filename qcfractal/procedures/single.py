"""
Procedure for a single computational task (energy, gradient, etc)
"""

from typing import List, Optional

import qcelemental as qcel
import qcengine as qcng

from .base import BaseTasks
from ..interface.models import Molecule, ResultRecord, TaskRecord, KeywordSet
from ..interface.models.task_models import PriorityEnum

_wfn_return_names = set(qcel.models.results.WavefunctionProperties._return_results_names)
_wfn_all_fields = set(qcel.models.results.WavefunctionProperties.__fields__.keys())


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
        assert procedure.lower() == "single"

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
            r = all_result_records[idx].copy(update={"id": all_result_ids[idx]})
            all_result_records[idx] = r

        # Now generate all the tasks, but only for results that don't exist already
        self.create_tasks(
            all_result_records, valid_molecules, [keywords] * len(all_result_records), tag=tag, priority=priority
        )

        # Keep the returned result id list in the same order as the input molecule list
        # If a molecule was None, then the corresponding result ID will be None
        # (since the entry in valid_molecule_idx will be missing). Ditto for molecules specified
        # more than once in the argument to this function
        results_ids = [None] * len(molecule_list)
        for idx, result_id in zip(valid_molecule_idx, all_result_ids):
            results_ids[idx] = result_id

        return results_ids, existing_ids

    def create_tasks(
        self,
        records: List[ResultRecord],
        molecules: Optional[List[Molecule]] = None,
        keywords: Optional[List[KeywordSet]] = None,
        tag: Optional[str] = None,
        priority: Optional[PriorityEnum] = None,
    ):
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

        return self.storage.queue_submit(new_tasks)

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
                    self.logger.warning(f"Too much wavefunction data for result {base_id}, removing extra data.")
                    available_keys &= _wfn_all_fields

                wavefunction_save = {k: wfn[k] for k in available_keys}
                wfn_data_id = self.storage.add_wavefunction_store([wavefunction_save])["data"][0]
                rdata["wavefunction_data_id"] = wfn_data_id

            # Create an updated ResultRecord based on the existing record and the new results
            # Double check to make sure everything is consistent
            assert existing_result["method"] == rdata["model"]["method"]
            assert existing_result["basis"] == rdata["model"]["basis"]
            assert existing_result["driver"] == rdata["driver"]
            assert existing_result["molecule"] == rdata["molecule"]["id"]

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
        self.storage.queue_mark_complete(completed_tasks)

        return completed_tasks

    @staticmethod
    def _build_schema_input(
        record: ResultRecord, molecule: "Molecule", keywords: Optional["KeywordSet"] = None
    ) -> "ResultInput":
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
