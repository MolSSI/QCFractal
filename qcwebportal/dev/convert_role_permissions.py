#!/usr/bin/env python3

# Converts the server's global role permissions (yaml) into the json file used by the webapp.
# With --check, verifies that the json file is up to date instead of writing it.

import argparse
import json
import os
import sys

import yaml

my_dir = os.path.dirname(os.path.abspath(__file__))
in_path = os.path.normpath(
    os.path.join(my_dir, "..", "..", "qcfractal", "qcfractal", "components", "auth", "global_role_permissions.yaml")
)
out_path = os.path.normpath(os.path.join(my_dir, "..", "src", "global_role_permissions.json"))

parser = argparse.ArgumentParser()
parser.add_argument("--check", action="store_true", help="Check that the json file is up to date")
args = parser.parse_args()

with open(in_path, "r") as f:
    data = yaml.safe_load(f)

if args.check:
    with open(out_path, "r") as f:
        existing = json.load(f)
    if existing != data:
        print(f"{out_path} is out of date with {in_path}. Run {__file__} to regenerate it.")
        sys.exit(1)
    print("Role permissions are up to date")
else:
    with open(out_path, "wt") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
