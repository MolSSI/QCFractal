import json
import lzma
import sys

from qcarchivetesting.data_generator import DataGeneratorComputeProcess
from qcfractal.components.reaction.testing_helpers import generate_task_key
from qcfractal.process_runner import ProcessRunner
from qcfractal.snowflake import FractalSnowflake
from qcportal.molecules import Molecule
from qcportal.serialization import _JSONEncoder

if len(sys.argv) != 2:
    raise RuntimeError("Script takes a single argument - path to a test data input file")

infile_name = sys.argv[1]
outfile_name = infile_name + ".xz"

# Load the start of the test data
print(f"** Reading in data from {infile_name}")

with open(sys.argv[1]) as infile:
    test_data = json.load(infile)


# Set up the snowflake and compute process
print(f"** Starting snowflake")
snowflake = FractalSnowflake(compute_workers=0)
client = snowflake.client()
config = snowflake._qcf_config

# Add the data
stoichiometry = [(x, Molecule(**y)) for x, y in test_data["stoichiometry"]]
_, ids = client.add_reactions(
    [stoichiometry],
    program=test_data["specification"]["program"],
    singlepoint_specification=test_data["specification"]["singlepoint_specification"],
    optimization_specification=test_data["specification"]["optimization_specification"],
    keywords=test_data["specification"]["keywords"],
)

record_id = ids[0]

print(f"** Starting compute")
compute = DataGeneratorComputeProcess(config, compute_workers=3)
compute_proc = ProcessRunner("snowflake_compute", compute, False)
compute_proc.start()

print(f"** Waiting for computation to finish")
result_data = {}
while True:
    finished = snowflake.await_results([record_id], timeout=60)
    result_data.update(compute.get_data())

    if finished:
        break

print("** Computation complete. Assembling results **")
record = client.get_records(record_id)
if record.status != "complete":
    print(record.error)
    errs = client.query_records(status="error")
    for x in errs:
        print(x.error)
    raise RuntimeError(f"Record status is {record.status}")

test_data["results"] = {}
for record_id, result in result_data.items():
    if result["schema_name"] == "qcschema_optimization_output":
        record = client.get_optimizations(record_id, include=["initial_molecule"])
    else:
        record = client.get_singlepoints(record_id, include=["molecule"])

    task_key = generate_task_key(record.raw_data.dict())
    test_data["results"][task_key] = result

print(f"** Writing output to {outfile_name}")
with lzma.open(outfile_name, "wt") as f:
    json.dump(test_data, f, cls=_JSONEncoder, indent=4, sort_keys=True)

print(f"** Stopping compute worker")
compute_proc.stop()