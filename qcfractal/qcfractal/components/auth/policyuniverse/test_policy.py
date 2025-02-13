"""
Credit: https://github.com/Netflix-Skunkworks/policyuniverse
Patrick Kelley <patrick@netflix.com>

"""

from .policy import Policy


policy01 = dict(
    Version="2012-10-08",
    Statement=dict(
        Effect="Allow",
        Principal="*",
        Action=["rds:*"],
        Resource="*",
    ),
)

policy02 = dict(
    Version="2010-08-14",
    Statement=[
        dict(
            Effect="Allow",
            Principal="arn:aws:iam::012345678910:root",
            Action=["rds:*"],
            Resource="*",
        )
    ],
)

# One statement limits by ARN, the other allows any account number
policy03 = dict(
    Version="2010-08-14",
    Statement=[
        dict(
            Effect="Allow",
            Principal="arn:aws:iam::012345678910:root",
            Action=["s3:*"],
            Resource="*",
        ),
        dict(
            Effect="Allow",
            Principal="arn:aws:iam::*:role/Hello",
            Action=["ec2:*"],
            Resource="*",
        ),
    ],
)

policy04 = dict(
    Version="2010-08-14",
    Statement=[
        dict(
            Effect="Allow",
            Principal="arn:aws:iam::012345678910:root",
            Action=["s3:*"],
            Resource="*",
        ),
        dict(
            Effect="Allow",
            Principal="arn:aws:iam::*:role/Hello",
            Action=["ec2:*"],
            Resource="*",
        ),
    ],
)

policy05 = dict(
    Version="2010-08-14",
    Statement=[
        dict(
            Effect="Allow",
            Principal="arn:aws:iam::012345678910:root",
            Action=["s3:*"],
            Resource="*",
        ),
        dict(
            Effect="Allow",
            Principal="arn:aws:iam::*:role/Hello",
            Action=["ec2:*"],
            Resource="*",
        ),
    ],
)

# AWS Organizations
policy06 = dict(
    Version="2010-08-14",
    Statement=[
        dict(
            Effect="Allow",
            Principal="*",
            Action=["rds:*"],
            Resource="*",
        )
    ],
)


def test_principals():
    assert Policy(policy04).principals == set(["arn:aws:iam::012345678910:root", "arn:aws:iam::*:role/Hello"])


def test_whos_allowed():
    allowed = Policy(policy03).whos_allowed()
    assert len(allowed) == 2

    allowed = Policy(policy04).whos_allowed()
    assert len(allowed) == 2
    principal_allowed = set([item for item in allowed if item.category == "principal"])
    assert len(principal_allowed) == 2

    allowed = Policy(policy06).whos_allowed()
    assert len(allowed) == 1
