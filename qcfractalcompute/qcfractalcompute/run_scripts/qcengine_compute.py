import json
import sys

import qcengine

if __name__ == "__main__":

    function_kwargs_file = sys.argv[1]

    with open(function_kwargs_file, "r") as f:
        function_kwargs = json.load(f)

    if "procedure" in function_kwargs:
        ret = qcengine.compute_procedure(**function_kwargs)
    else:
        ret = qcengine.compute(**function_kwargs)

    print(json.dumps(ret.dict(encoding="json")))
