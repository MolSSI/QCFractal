"""
Utility functions for on-node procedures.
"""

import json

from typing import Optional, Dict, Any

from qcelemental.models import ResultInput

from ..interface.models import Molecule, QCSpecification


def unpack_single_task_spec(storage, meta, molecules):
    """Transforms a metadata compute packet into an expanded
    QC Schema for multiple runs.

    Parameters
    ----------
    storage : DBSocket
        A live connection to the current database.
    meta : dict
        A JSON description of the metadata involved with the computation
    molecules : list of str, dict
        A list of molecule ID's or full JSON molecules associated with the run.

    Returns
    -------
    ret : tuple(dict, list)
        A dictionary of JSON representations with keys built in.
        The list is an array of any errors occurred

    Examples
    --------

    >>> meta = {
        "procedure": "single",
        "driver": "energy",
        "method": "HF",
        "basis": "sto-3g",
        "keywords": "default",
        "program": "psi4",
    }

    >>> molecules = [{"geometry": [0, 0, 0], "symbols" : ["He"]}]

    >>> unpack_single_task_spec(storage, meta, molecules)

    """

    # Get the required molecules
    raw_molecules_query = storage.get_add_molecules_mixed(molecules)

    # Pull out the needed keywords
    if meta["keywords"] is None:
        keyword_set = {}
    else:
        keyword_set = storage.get_add_keywords_mixed([meta["keywords"]])["data"][0]
        keyword_set = keyword_set["values"]

    # Create the "universal header"
    task_meta = json.dumps(
        {
            "driver": meta["driver"],
            "keywords": keyword_set,
            "model": {"method": meta["method"], "basis": meta["basis"]},
        }
    )

    tasks = []
    for mol in raw_molecules_query["data"]:
        if mol is None:
            tasks.append(None)
            continue

        data = json.loads(task_meta)
        data["molecule"] = mol

        tasks.append(ResultInput(**data))

    return tasks, []


def parse_single_tasks(storage, results, qc_spec):
    """Summary

    Parameters
    ----------
    storage : DBSocket
        A live connection to the current database.
    results : dict
        A (key, result) dictionary of the single return results.

    Returns
    -------

    Examples
    --------

    """

    for k, v in results.items():
        # Flatten data back out
        v["method"] = v["model"]["method"]
        v["basis"] = v["model"]["basis"]
        del v["model"]

        # Molecule should be by ID
        v["molecule"] = storage.add_molecules([Molecule(**v["molecule"])])["data"][0]

        v["keywords"] = qc_spec.keywords
        v["program"] = qc_spec.program

        # Old tags that may still exist if the task was created with previous versions.
        # It is harmless if they do, but may as well do a consistency check
        if "_qcfractal_tags" in v["extras"]:
            assert int(v["extras"]["_qcfractal_tags"]["keywords"]) == int(qc_spec.keywords)
            assert v["extras"]["_qcfractal_tags"]["program"] == qc_spec.program
            del v["extras"]["_qcfractal_tags"]

        del v["schema_name"]
        del v["schema_version"]

        if v.pop("success"):
            v["status"] = "COMPLETE"
        else:
            v["status"] = "ERROR"

    return results


def form_qcinputspec_schema(qc_spec: QCSpecification, keywords: Optional["KeywordSet"] = None) -> Dict[str, Any]:
    if qc_spec.keywords:
        assert keywords.id == qc_spec.keywords

    # Note: program is unused in QCInputSpecification
    ret = {
        "driver": str(qc_spec.driver.name),
        "model": {"method": qc_spec.method},
    }  # yapf: disable
    if qc_spec.basis:
        ret["model"]["basis"] = qc_spec.basis

    if keywords:
        ret["keywords"] = keywords.values
    else:
        ret["keywords"] = {}

    return ret
