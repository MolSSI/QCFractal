import logging
import sys

from qcarchivetesting.data_generator import DataGeneratorComputeThread, read_input, write_outputs
from qcfractal.snowflake import FractalSnowflake
from qcportal.molecules import Molecule

logging.basicConfig(level=logging.WARNING)

if len(sys.argv) != 2:
    raise RuntimeError("Script takes a single argument - path to a test data input file")

infile_name = sys.argv[1]
test_data, outfile_name = read_input(infile_name)

# Set up the snowflake and compute process
print(f"** Starting snowflake")
snowflake = FractalSnowflake(compute_workers=0)
client = snowflake.client()
config = snowflake._qcf_config

# Add the data
_, ids = client.add_singlepoints(
    [Molecule(**test_data["molecule"])],
    program=test_data["specification"]["program"],
    driver=test_data["specification"]["driver"],
    method=test_data["specification"]["method"],
    basis=test_data["specification"]["basis"],
    keywords=test_data["specification"]["keywords"],
    protocols=test_data["specification"]["protocols"],
)

record_id = ids[0]

print(f"** Starting compute")
compute = DataGeneratorComputeThread(config, n_workers=1)

print(f"** Waiting for computation to finish")
snowflake.await_results([record_id], timeout=None)

print("** Computation complete. Assembling results **")
record = client.get_singlepoints(record_id, include=["**"])
if record.status != "complete":
    print(record.error)
    errs = client.query_records(status="error")
    for x in errs:
        print(x.error)
    raise RuntimeError(f"Record status is {record.status}")

result_data = compute.get_data()
assert len(result_data) == 1

task, result = result_data[0]
assert task.record_id == record_id
assert result["success"] is True

test_data["result"] = result

write_outputs(outfile_name, test_data, record)

print(f"** Stopping compute worker")
compute.stop()
