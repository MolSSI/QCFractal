from qcfractal.components.auth.role_permissions import AuthorizedEnum, GLOBAL_ROLE_PERMISSIONS


def evaluate_global_permissions(role: str, resource: str, action: str) -> AuthorizedEnum:
    # Always paranoid about auth stuff
    if not isinstance(role, str) or len(role) == 0:
        raise TypeError("The role must be a string with length > 0")
    if not isinstance(resource, str) or len(resource) == 0:
        raise TypeError("The resource must be a string with length > 0")
    if not isinstance(action, str) or len(action) == 0:
        raise TypeError("The action must be a string with length > 0")

    if role not in GLOBAL_ROLE_PERMISSIONS:
        return AuthorizedEnum.Deny

    role_permissions = GLOBAL_ROLE_PERMISSIONS[role]

    # Check if there is an entry for this resource or if there is a wildcard entry for all resources.
    # If not, deny by default.
    if resource in role_permissions:
        resource_permissions = role_permissions[resource]
    elif "*" in role_permissions:
        resource_permissions = role_permissions["*"]
    else:
        return AuthorizedEnum.Deny

    # Now check the action (or wildcard) and return whatever is there
    if action in resource_permissions:
        return resource_permissions[action]
    elif "*" in resource_permissions:
        return resource_permissions["*"]

    return AuthorizedEnum.Deny
