import json

# This script returns the programs/procedures available in qcengine
# as a dictionary of {program: version}
# It is meant to be used with subprocess to get the available programs
# in a conda environment

if __name__ == "__main__":
    try:
        import qcengine

        progs = {x: qcengine.get_program(x).get_version() for x in qcengine.list_available_programs()}
        procs = {x: qcengine.get_procedure(x).get_version() for x in qcengine.list_available_procedures()}
        progs["qcengine"] = qcengine.__version__

    except ImportError:
        progs = {}
        procs = {}

    print(json.dumps({**progs, **procs}))
