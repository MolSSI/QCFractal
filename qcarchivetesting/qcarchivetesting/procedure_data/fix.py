import json
import sys

with open(sys.argv[1]) as f:
    d = json.load(f)

d.pop("result", None)
d.pop("results", None)

with open(sys.argv[1], "w") as f:
    json.dump(d, f, indent=2)
