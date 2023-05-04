import json
import logging
import lzma
import sys

from qcarchivetesting.data_generator import DataGeneratorComputeThread
from qcfractal.snowflake import FractalSnowflake
from qcportal.molecules import Molecule
from qcportal.serialization import _JSONEncoder

logging.basicConfig(level=logging.WARNING)

if len(sys.argv) != 2:
    raise RuntimeError("Script takes a single argument - path to a test data input file")

infile_name = sys.argv[1]
outfile_name = infile_name + ".xz"

# Load the start of the test data
print(f"** Reading in data from {infile_name}")

with open(infile_name) as infile:
    test_data = json.load(infile)


# Set up the snowflake and compute process
print(f"** Starting snowflake")
snowflake = FractalSnowflake(compute_workers=0)
client = snowflake.client()
config = snowflake._qcf_config

# Add the data
_, ids = client.add_optimizations(
    [Molecule(**test_data["initial_molecule"])],
    program=test_data["specification"]["program"],
    qc_specification=test_data["specification"]["qc_specification"],
    keywords=test_data["specification"]["keywords"],
    protocols=test_data["specification"]["protocols"],
)

record_id = ids[0]

print(f"** Starting compute")
compute = DataGeneratorComputeThread(config, n_workers=1)

print(f"** Waiting for computation to finish")
snowflake.await_results([record_id], timeout=None)

print("** Computation complete. Assembling results **")
record = client.get_records(record_id)
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

print(f"** Writing output to {outfile_name}")
with lzma.open(outfile_name, "wt") as f:
    json.dump(test_data, f, cls=_JSONEncoder, indent=4, sort_keys=True)

print(f"** Stopping compute worker")
compute.stop()
