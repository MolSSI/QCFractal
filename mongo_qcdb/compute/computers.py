"""
Aggregates all of the computer classes for DatenQM
"""

from .psi4 import psi_compute

def pass_compute(json_data, **kwargs):
    """
    This doesnt do anything with the json. Used for testing.
    """

    json_data["success"] = True
    return json_data

computers = {"psi4": psi_compute, "pass": pass_compute}

if __name__ == "__main__":

    json_data = {}
    json_data["molecule"] = """He 0 0 0\n--\nHe 0 0 1"""
    json_data["driver"] = "energy"
    json_data["method"] = 'SCF'
    #json_data["kwargs"] = {"bsse_type": "cp"}
    json_data["options"] = {"BASIS": "STO-3G"}
    json_data["return_output"] = True

    ret = psi_compute(json_data)
    print(ret)

    print(ret["success"])
    print(ret["memory"])
