def _verify_input(data, mongo, logger=None, options=None):

    if options is not None:
        data["options"] = options

    # Check if the minimum is present
    for req in ["molecule_hash", "modelchem", "options"]:
        if req not in list(data):
            err = "Missing required field '%s'" % req
            data["error"] = err
            if logger:
                logger.info("SCHEDULER: %s" % err)
            return data

    # Grab out molecule
    mol = mongo.get_molecule(data["molecule_hash"])
    if mol is None:
        err = "Molecule hash '%s' was not found." % data["molecule_hash"]
        data["error"] = err
        if logger:
            logger.info("SCHEDULER: %s" % err)
        return data

    molecule_str = molecule.Molecule(mol, dtype="json").to_string(dtype="psi4")

    data["molecule"] = molecule_str
    data["method"] = data["modelchem"]
    data["driver"] = "energy"

    return data


def _unpack_tasks(data, mongo, logger):
    # Parse out data
    program = "psi4"
    tasks = []

    # Multiple jobs
    if ("multi_header" in list(data)) and (data["multi_header"] == "QCDB_batch"):
        for task in data["tasks"]:
            tasks.append(_verify_input(task, mongo, options=data["options"], logger=logger))
        program = data["program"]

    # Single job
    else:
        tasks.append(_verify_input(data, mongo, logger=logger))
        if "program" in list(data):
            program = data["program"]

    return tasks, program
