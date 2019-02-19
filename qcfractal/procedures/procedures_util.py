"""
Utility functions for on-node procedures.
"""

import hashlib
import json

from ..interface.models.common_models import ResultInput


def format_result_indices(data, program=None):
    if program is None:
        program = data["program"]
    return program, data["molecule"], data["driver"], data["method"], data["basis"], data["options"]


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
        "program": meta["program"],
        "driver": meta["driver"],
        "keywords": keyword_set,
        "model": {
            "method": meta["method"],
            "basis": meta["basis"]
        },
        "qcfractal_tags": {
            "program": meta["program"],
            "keywords": meta["keywords"]
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

        # Flatten data back out
        v["method"] = v["model"]["method"]
        v["basis"] = v["model"]["basis"]
        del v["model"]

        v["keywords"] = v["qcfractal_tags"]["keywords"]

        # Molecule should be by ID
        v["molecule"] = storage.add_molecules([v["molecule"]])["data"][0]

        v["program"] = v["qcfractal_tags"]["program"]

        del v["qcfractal_tags"]
    return results


def hash_single_task_spec(data, program=None):

    single_keys = format_result_indices(data, program=program)
    keys = {"procedure_type": "single", "single_key": single_keys}
    hash_index = hash_procedure_keys(keys)
    return keys, hash_index


def hash_procedure_keys(keys):
    m = hashlib.sha1()
    m.update(json.dumps(keys, sort_keys=True).encode("UTF-8"))
    return m.hexdigest()


def parse_hooks(rdata, rhooks):
    """Parses the hook data in a list of hooks
    TODO: this methos an has error, results is undefined

    Parameters
    ----------
    rdata : dict
        A {uid : results} dictionary of results to pull id's from
    rhooks : dict
        A {uid : hook} dictionary to apply the hooks too

    Returns
    -------
    TYPE
        Description
    """
    hook_data = []
    for k, hook in rhooks.items():

        # If no hooks skip it
        if len(hook) == 0:
            continue

        # Loop over hooks
        for h in hook:
            # Loop over individual commands
            for command in h["updates"]:
                # Custom commands
                if not isinstance(command[-1], str):
                    continue
                elif "$" not in command[-1]:
                    continue
                elif command[-1] == "$task_id":
                    command[-1] = results[k]["id"]
                elif command[-1] == "$hash_index":
                    command[-1] = results[k]["hash_index"]
                else:
                    raise KeyError("Hook command `{}` not understood.".format(command))

        hook_data.append(hook)
    return hook_data
