#!/usr/bin/env python3

import os
import yaml
import json


my_dir = os.path.dirname(os.path.abspath(__file__))
out_path = os.path.join("../src/global_role_permissions.json")

with open("global_role_permissions.yaml", "r") as f:
    data = yaml.safe_load(f)

with open(out_path, 'wt') as f:
    json.dump(data, f, indent=2)
