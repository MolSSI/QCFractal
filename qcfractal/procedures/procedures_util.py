"""
Utility functions for on-node procedures.
"""

import json

def unpack_single_run_meta(db, meta, molecules):
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

    >>> unpack_single_run_meta(db, meta, molecules)


    """

    compute = {
        "meta": {
            "procedure": "single",
            "driver": "energy",
            "method": "HF",
            "basis": "sto-3g",
            "options": "default",
            "program": "psi4",
        },
        "data": [mol_ret["data"]["hydrogen"]],
    }
    # Dumps is faster than copy
    task_meta = json.dumps({k: meta[k] for k in ["program", "driver", "method", "basis", "options"]})

    tasks = {}
    errors = []
    for mol in molecules:
        data = json.loads(task_meta)
        data["molecule_id"] = mol

        tasks[schema.format_result_indices(data)] = data

    # Pull out the needed molecules
    needed_mols = list({x["molecule_id"] for x in tasks.values()})
    raw_molecules = db.get_molecules(needed_mols, index="id")
    molecules = {x["id"]: x for x in raw_molecules["data"]}

    # Add molecules back into tasks
    for k, v in tasks.items():
        if v["molecule_id"] in molecules:
            v["molecule"] = molecules[v["molecule_id"]]
            del v["molecule_id"]
        else:
            errors.append((k, "Molecule not found"))
            del tasks[k]

    # Pull out the needed options
    option_set = db.get_options([(self.json["meta"]["program"], self.json["meta"]["options"])])["data"][0]
    del option_set["name"]
    del option_set["program"]

    # Add options back into tasks
    for k, v in tasks.items():
        v["keywords"] = option_set
        del v["options"]

    # Build out full and complete task list
    full_tasks = {}
    for k, v in tasks.items():
        # Reformat model syntax
        v["schema_name"] = "qc_schema_input"
        v["schema_version"] = 1
        v["model"] = {"method": v["method"], "basis": v["basis"]}
        del v["method"]
        del v["basis"]

        full_tasks[k] = (qcengine.compute, v, self.json["meta"]["program"])

    return (tasks, errors)

def parse_single_runs(db, results):
    """Summary

    Parameters
    ----------
    db : TYPE
        Description
    results : TYPE
        Description

    Returns
    -------

    Examples
    --------

    """

    # Get molecule ID's
    mols = {k: v["molecule"] for k, v in results.items()}
    mol_ret = self.db_socket.add_molecules(mols)["data"]

    for k, v in results.items():

        # Flatten data back out
        v["method"] = v["model"]["method"]
        v["basis"] = v["model"]["basis"]
        del v["model"]

        v["options"] = k[-1]
        del v["keywords"]

        v["molecule_id"] = mol_ret[k]
        del v["molecule"]

        v["program"] = k[0]

    return results
