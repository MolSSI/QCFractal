import os
from enum import Enum

import yaml
from pydantic import TypeAdapter


class AuthorizedEnum(str, Enum):
    Allow = "Allow"
    Deny = "Deny"
    Conditional = "Conditional"


_my_path = os.path.dirname(os.path.abspath(__file__))
_global_perm_file_path = os.path.join(_my_path, "global_role_permissions.yaml")

with open(_global_perm_file_path, "r") as f:
    file_data = yaml.safe_load(f)

GLOBAL_ROLE_PERMISSIONS = TypeAdapter(dict[str, dict[str, dict[str, AuthorizedEnum]]]).validate_python(file_data)
