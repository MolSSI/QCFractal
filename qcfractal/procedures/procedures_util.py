"""
Utility functions for on-node procedures.
"""

import copy
import hashlib
import json

from .. import interface


def unpack_single_run_meta(storage, meta, molecules, remove_duplicates=True):
    """Transforms a metadata compute packet into an expanded
    QC Schema for multiple runs.

    Parameters
    ----------
    db : DBSocket
        A live connection to the current database.
    meta : dict
        A JSON description of the metadata involved with the computation
    molecules : list of str, dict
        A list of molecule ID's or full JSON molecules associated with the run.

    Returns
    -------
    ret : tuple(dict, list)
        A dictionary of JSON representations with keys built in.

    Examples
    --------

    >>> meta = {
        "procedure": "single",
        "driver": "energy",
        "method": "HF",
        "basis": "sto-3g",
        "options": "default",
        "program": "psi4",
    }

    >>> molecules = [{"geometry": [0, 0, 0], "symbols" : ["He"]}]

    >>> unpack_single_run_meta(storage, meta, molecules)


    """

    # Get the required molecules
    indexed_molecules = {k: v for k, v in enumerate(molecules)}
    raw_molecules_query = storage.mixed_molecule_get(indexed_molecules)

    # Pull out the needed options
    completed = set()
    if remove_duplicates:
        query = copy.deepcopy(meta)
        query["molecule_id"] = [x["id"] for x in raw_molecules_query["data"].values()]
        del query["procedure"]

        search = storage.get_results(query, projection={"molecule_id": True})
        completed = set(x["molecule_id"] for x in search["data"])

    option_set = storage.get_options([(meta["program"], meta["options"])])["data"][0]
    del option_set["name"]
    del option_set["program"]

    # Create the "universal header"
    task_meta = json.dumps({
        "schema_name": "qc_schema_input",
        "schema_version": 1,
        "program": meta["program"],
        "driver": meta["driver"],
        "keywords": option_set,
        "model": {
            "method": meta["method"],
            "basis": meta["basis"]
        },
        "qcfractal_tags": {
            "program": meta["program"],
            "options": meta["options"]
        }
    })


    tasks = {}
    indexer = copy.deepcopy(meta)
    for idx, mol in raw_molecules_query["data"].items():
        if mol["id"] in completed:
            continue

        data = json.loads(task_meta)
        data["molecule"] = mol

        indexer["molecule_id"] = mol["id"]
        tasks[interface.schema.format_result_indices(indexer)] = data

    return (tasks, completed, [])


def parse_single_runs(storage, results):
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

    # Get molecule ID's
    mols = {k: v["molecule"] for k, v in results.items()}
    mol_ret = storage.add_molecules(mols)["data"]

    for k, v in results.items():

        # Flatten data back out
        v["method"] = v["model"]["method"]
        v["basis"] = v["model"]["basis"]
        del v["model"]

        v["options"] = v["qcfractal_tags"]["options"]
        del v["keywords"]

        v["molecule_id"] = mol_ret[k]
        del v["molecule"]

        v["program"] = v["qcfractal_tags"]["program"]

        del v["qcfractal_tags"]

    return results


def hash_procedure_keys(keys):
    m = hashlib.sha1()
    m.update(json.dumps(keys, sort_keys=True).encode("UTF-8"))
    return m.hexdigest()

def parse_hooks(data, results):
    """Parses the hook data in a list of hooks

    Parameters
    ----------
    data : dict
        Dictionary of key/value results from a queue adapter. key : (data blob, hook)
    results : dict
        Parsed versions of results inserted with ID's attached.

    Returns
    -------
    TYPE
        Description
    """
    hook_data = []
    for k, (data, hook) in data.items():

        # If no hooks skip it
        if len(hook) == 0:
            continue

        # Loop over hooks
        for h in hook:
            # Loop over individual commands
            for command in h["updates"]:
                # Custom commands
                if command[-1] == "$task_id":
                    command[-1] = results[k]["id"]
                # else:
                #     raise KeyError("Hook command `{}` not understood.".format(command))

        hook_data.append(hook)
    return hook_data
