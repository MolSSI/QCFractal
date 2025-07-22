import logging
import sys

from qcarchivetesting.data_generator import DataGeneratorComputeThread
from qcarchivetesting.data_generator import read_input, write_outputs
from qcfractal.components.manybody.testing_helpers import generate_task_key
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
molecule = Molecule(**test_data["molecule"])
_, ids = client.add_manybodys(
    [molecule],
    program=test_data["specification"]["program"],
    bsse_correction=test_data["specification"]["bsse_correction"],
    levels=test_data["specification"]["levels"],
    keywords=test_data["specification"]["keywords"],
)

record_id = ids[0]

print(f"** Starting compute")
compute = DataGeneratorComputeThread(config, n_workers=3)

print(f"** Waiting for computation to finish")
result_data = []
while True:
    finished = snowflake.await_results([record_id], timeout=60)
    result_data.extend(compute.get_data())

    if finished:
        break

print("** Computation complete. Assembling results **")
record = client.get_manybodys(record_id, include=["**"])
record.fetch_children(include=["**"], force_fetch=True)

if record.status != "complete":
    print(record.error)
    errs = client.query_records(status="error")
    for x in errs:
        print(x.error)
    raise RuntimeError(f"Record status is {record.status}")

test_data["results"] = {}
for task, result in result_data:
    task_key = generate_task_key(task)
    test_data["results"][task_key] = result

write_outputs(outfile_name, test_data, record)

print(f"** Stopping compute worker")
compute.stop()
