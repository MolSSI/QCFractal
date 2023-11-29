import json
import sys

import qcengine

# From psi4/driver/p4util/python_helpers.py
# https://github.com/psi4/psi4/blob/master/psi4/driver/p4util/python_helpers.py
_qcvar_transitions = {
    "scsn-mp2 correlation energy": "scs(n)-mp2 correlation energy",
    "scsn-mp2 total energy": "scs(n)-mp2 total energy",
    "mayer_indices": "mayer indices",
    "wiberg_lowdin_indices": "wiberg lowdin indices",
    "lowdin_charges": "lowdin charges",
    "mulliken_charges": "mulliken charges",
    "(at) correction energy": "a-(t) correction energy",
    "ccsd(at) total energy": "a-ccsd(t) total energy",
    "ccsd(at) correlation energy": "a-ccsd(t) correlation energy",
    "cp-corrected 2-body interaction energy": "cp-corrected interaction energy through 2-body",
    "cp-corrected 3-body interaction energy": "cp-corrected interaction energy through 3-body",
    "cp-corrected 4-body interaction energy": "cp-corrected interaction energy through 4-body",
    "cp-corrected 5-body interaction energy": "cp-corrected interaction energy through 5-body",
    "nocp-corrected 2-body interaction energy": "nocp-corrected interaction energy through 2-body",
    "nocp-corrected 3-body interaction energy": "nocp-corrected interaction energy through 3-body",
    "nocp-corrected 4-body interaction energy": "nocp-corrected interaction energy through 4-body",
    "nocp-corrected 5-body interaction energy": "nocp-corrected interaction energy through 5-body",
    "vmfc-corrected 2-body interaction energy": "vmfc-corrected interaction energy through 2-body",
    "vmfc-corrected 3-body interaction energy": "vmfc-corrected interaction energy through 3-body",
    "vmfc-corrected 4-body interaction energy": "vmfc-corrected interaction energy through 4-body",
    "vmfc-corrected 5-body interaction energy": "vmfc-corrected interaction energy through 5-body",
    "counterpoise corrected total energy": "cp-corrected total energy",
    "counterpoise corrected interaction energy": "cp-corrected interaction energy",
    "non-counterpoise corrected total energy": "nocp-corrected total energy",
    "non-counterpoise corrected interaction energy": "nocp-corrected interaction energy",
    "valiron-mayer function couterpoise total energy": "valiron-mayer function counterpoise total energy",  # note misspelling
    "valiron-mayer function couterpoise interaction energy": "vmfc-corrected interaction energy",  # note misspelling
}

if __name__ == "__main__":
    function_kwargs_file = sys.argv[1]

    with open(function_kwargs_file, "r") as f:
        function_kwargs = json.load(f)

    if "procedure" in function_kwargs:
        ret = qcengine.compute_procedure(**function_kwargs)
    else:
        ret = qcengine.compute(**function_kwargs)

        # Hacky - handle keys in qcvars
        if ret.success and ret.extras and "qcvars" in ret.extras:
            # Make qcvars keys all lowercase
            ret.extras["qcvars"] = {k.lower(): v for k, v in ret.extras["qcvars"].items()}

            # Remove any from qcvars that are in properties (but with underscores)
            prop_dict = ret.properties.dict()
            to_delete = [k for k in ret.extras["qcvars"].keys() if k.replace(" ", "_") in prop_dict.keys()]
            for k in to_delete:
                ret.extras["qcvars"].pop(k)

            # Replace any names with underscores (and other modifications)
            ret.extras["qcvars"] = {_qcvar_transitions.get(k, k): v for k, v in ret.extras["qcvars"].items()}

    print(json.dumps(ret.dict(encoding="json")))
