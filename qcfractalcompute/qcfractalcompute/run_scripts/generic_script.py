import importlib
import io
import json
import operator
import sys
import time
from contextlib import redirect_stdout, redirect_stderr

if __name__ == "__main__":
    function_call_data_file = sys.argv[1]

    with open(function_call_data_file, "r") as f:
        function_call_data = json.load(f)

    module_name, func_name = function_call_data["function"].split(".", 1)
    module = importlib.import_module(module_name)
    func = operator.attrgetter(func_name)(module)

    with redirect_stdout(io.StringIO()) as rdout:
        with redirect_stderr(io.StringIO()) as rderr:
            start_time = time.time()
            try:
                results = func(**function_call_data["function_kwargs"])
            except Exception as e:
                ret = {
                    "error": {
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                    },
                    "success": False,
                }

            end_time = time.time()

    stdout = rdout.getvalue()
    stderr = rderr.getvalue()

    ret = {
        "id": function_call_data["record_id"],
        "success": True,
        "stdout": stdout if stdout else None,  # convert empty string to None
        "stderr": stderr if stderr else None,
        "results": results,
        "provenance": {
            "creator": function_call_data["function"],
            "walltime": (end_time - start_time),
        },
    }

    print(json.dumps(ret))
