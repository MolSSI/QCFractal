# Used in some migrations before changing how roles work

default_roles = {
    "admin": {
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
        ]
    },
    "maintain": {
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
        ]
    },
    "read": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": ["/api/v1/users", "/api/v1/me"]},
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": [
                    "/api/v1/roles",
                    "/api/v1/managers",
                    "/api/v1/server_errors",
                    "/api/v1/access_logs",
                    "/api/v1/tasks",
                    "/api/v1/internal_jobs",
                ],
            },
        ]
    },
    "monitor": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": "/api/v1/users"},
            {"Effect": "Deny", "Action": "*", "Resource": ["/api/v1/roles"]},
        ]
    },
    "compute": {
        "Statement": [
            {"Effect": "Allow", "Action": ["READ"], "Resource": "/api/v1/information"},
            {"Effect": "Allow", "Action": ["READ"], "Resource": "/compute/v1/information"},
            {"Effect": "Allow", "Action": ["READ", "WRITE"], "Resource": "/api/v1/users"},
            {"Effect": "Allow", "Action": "*", "Resource": ["/compute/v1/managers", "/compute/v1/tasks"]},
        ]
    },
    "submit": {
        "Statement": [
            {"Effect": "Allow", "Action": "READ", "Resource": "*"},
            {"Effect": "Allow", "Action": "WRITE", "Resource": "/api/v1/users"},
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": [
                    "/api/v1/roles",
                    "/api/v1/managers",
                    "/api/v1/server_errors",
                    "/api/v1/access_logs",
                    "/api/v1/tasks",
                    "/api/v1/internal_jobs",
                ],
            },
            {
                "Effect": "Allow",
                "Action": "*",
                "Resource": ["/api/v1/records", "/api/v1/molecules", "/api/v1/keywords", "/api/v1/datasets"],
            },
        ]
    },
}
