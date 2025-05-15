from typing import Dict, Set

GLOBAL_ROLE_PERMISSIONS: Dict[str, Dict[str, Set[str]]] = {
    "admin": {
        "*": {"*"},
    },
    "maintain": {
        "information": {"read", "modify"},
        "me": {"read", "modify"},
        "users": {"read", "modify"},
        "groups": {"*"},
        "access_log": {"*"},
        "server_errors": {"*"},
        "internal_jobs": {"*"},
        "managers": {"read"},
        "records": {"*"},
        "datasets": {"*"},
        "projects": {"*"},
    },
    "monitor": {
        "information": {"read"},
        "me": {"read", "modify"},
        "users": {"read", "modify"},  # TODO - DEPRECATE
        "managers": {"read"},
        "records": {"read"},
        "datasets": {"read"},
        "projects": {"read"},
        "access_log": {"read"},
        "server_errors": {"read"},
        "internal_jobs": {"read"},
    },
    "submit": {
        "information": {"read"},
        "me": {"read", "modify"},
        "users": {"read", "modify"},  # TODO - DEPRECATE
        "managers": {"read"},
        "records": {"read", "add", "modify", "delete"},
        "datasets": {"read", "add", "modify", "delete", "create_view"},
        "projects": {"read", "add", "modify", "delete"},
    },
    "read": {
        "information": {"read"},
        "me": {"read", "modify"},
        "users": {"read", "modify"},  # TODO - DEPRECATE
        "managers": {"read"},
        "records": {"read"},
        "datasets": {"read"},
        "projects": {"read"},
    },
    "anonymous": {
        "information": {"read"},
        "managers": {"read"},
        "records": {"read"},
        "datasets": {"read"},
        "projects": {"read"},
    },
    "compute": {
        "information": {"read"},
        "me": {"read", "modify"},
        "managers": {"add", "modify"},
        "tasks": {"claim", "return"},
        "users": {"read", "modify"},  # TODO - DEPRECATE
    },
}
