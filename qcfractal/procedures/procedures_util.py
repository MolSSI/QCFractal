"""
Utility functions for on-node procedures.
"""

import json

from qcelemental.models import ResultInput
from ..interface.models import Molecule


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
    task_meta = json.dumps({
        "driver": meta["driver"],
        "keywords": keyword_set,
        "model": {
            "method": meta["method"],
            "basis": meta["basis"]
        },
        "extras": {
            "_qcfractal_tags": {
                "program": meta["program"],
                "keywords": meta["keywords"]
            }
        }
    })

    tasks = []
    for mol in raw_molecules_query["data"]:
        if mol is None:
            tasks.append(None)
            continue

        data = json.loads(task_meta)
        data["molecule"] = mol

        tasks.append(ResultInput(**data))

    return tasks, []


def parse_single_tasks(storage, results):
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
        stdout, stderr, error = storage.add_kvstore([v["stdout"], v["stderr"], v["error"]])["data"]
        v["stdout"] = stdout
        v["stderr"] = stderr
        v["error"] = error

        # Flatten data back out
        v["method"] = v["model"]["method"]
        v["basis"] = v["model"]["basis"]
        del v["model"]

        # Molecule should be by ID
        v["molecule"] = storage.add_molecules([Molecule(**v["molecule"])])["data"][0]

        v["keywords"] = v["extras"]["_qcfractal_tags"]["keywords"]
        v["program"] = v["extras"]["_qcfractal_tags"]["program"]
        del v["extras"]["_qcfractal_tags"]
        del v["schema_name"]
        del v["schema_version"]

        if v.pop("success"):
            v["status"] = "COMPLETE"
        else:
            v["status"] = "ERROR"

    return results
