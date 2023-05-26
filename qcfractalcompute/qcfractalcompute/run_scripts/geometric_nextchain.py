import logging
import io
import json
import sys
import time
import traceback
import re
from contextlib import redirect_stderr

import geometric

if __name__ == "__main__":

    record_id = sys.argv[1]
    nextchain_info_file = sys.argv[2]

    logger = logging.getLogger('geometric.nifty')
    rdout = io.StringIO()
    handler = logging.StreamHandler(rdout)
    handler.terminator = ""
    logger.addHandler(handler)

    with open(nextchain_info_file, "r") as f:
        nextchain_kwargs = json.load(f)

    with redirect_stderr(io.StringIO()) as rderr:
        start_time = time.time()
        success = False
        try:
            results = geometric.neb.nextchain(**nextchain_kwargs)
            success = True
        except Exception as e:
            success = False
            results = {
                "error": {
                    "error_type": type(e).__name__,
                    "error_message": traceback.format_exc(),
                },
                "success": False,
            }

        end_time = time.time()
    logger.handlers.clear()
    stdout = rdout.getvalue()
    stdout = re.sub("Custom engine selected.\n", "", stdout)
    stderr = rderr.getvalue()

    if success:
        ret = {
            "schema_name": "qca_generic_task_result",
            "id": record_id,
            "success": True,
            "stdout": stdout if stdout else None,  # convert empty string to None
            "stderr": stderr if stderr else None,
            "results": results,
            "extras": {},
            "provenance": {
                "creator": "geometric.neb.nextchain",
                "version": geometric.__version__,
                "walltime": (end_time - start_time),
            },
        }

        print(json.dumps(ret))
    else:
        results["error"]["error_message"] += f"\nstdout: {stdout}\n"
        results["error"]["error_message"] += f"\nstderr: {stderr}\n"
        print(json.dumps(results))
