"""
Credit: https://github.com/Netflix-Skunkworks/policyuniverse
Patrick Kelley <patrick@netflix.com>

"""

from .statement import Statement

# NotPrincipal
statement01 = dict(
    Effect="Allow",
    NotPrincipal={"AWS": ["arn:aws:iam::012345678910:root"]},
    Action=["rds:*"],
    Resource="*",
)

# "Principal": "value"
statement02 = dict(
    Effect="Allow",
    Principal="arn:aws:iam::012345678910:root",
    Action=["rds:*"],
    Resource="*",
)

statement03 = dict(
    Effect="Allow",
    Principal="arn:aws:iam::012345678910:root",
    Action=["rds:*"],
    Resource="*",
)

statement04 = dict(
    Effect="Allow",
    Principal=["arn:aws:iam::012345678910:root"],
    Action=["rds:*"],
    Resource="*",
)


statement06 = dict(
    Effect="Allow",
    Principal=["lambda.amazonaws.com"],
    Action=["rds:*"],
    Resource="*",
)

statement07 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement08 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement09 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement09_wildcard = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement10 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement11 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement12 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement13 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement14 = dict(Effect="Allow", Principal="*", Action=["rds:*"], Resource="*")

statement15 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement16 = dict(Effect="Deny", Principal="*", Action=["rds:*"], Resource="*")

# Bad ARN
statement17 = dict(
    Effect="Allow",
    Principal="arn:aws:iam::012345678910",
    Action=["rds:*"],
    Resource="*",
)

# ARN Like with wildcard account number
statement18 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

# StringLike with wildcard
statement19 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement20 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement21 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement22 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

statement23 = dict(
    Effect="Allow",
    Principal="*",
    Action=["kms:*"],
    Resource="*",
)

# Testing action groups
statement24 = dict(
    Effect="Allow",
    Principal="*",
    Action=["ec2:authorizesecuritygroupingress", "ec2:AuthorizeSecuritygroupEgress"],
)

# Testing action groups
statement25 = dict(
    Effect="Allow",
    Principal="*",
    Action=[
        "ec2:authorizesecuritygroupingress",
        "ec2:AuthorizeSecuritygroupEgress",
        "iam:putrolepolicy",
    ],
    Resource="*",
)

# Testing action groups
statement26 = dict(
    Effect="Allow",
    Principal="*",
    Action=["iam:putrolepolicy", "iam:listroles"],
    Resource="*",
)

# Testing ForAnyValue/ForAllValues without list
# Like statement 07, but this one shouldn't work
statement27 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

# Testing ForAnyValue/ForAllValues without list
# Like statement 10, but this one shouldn't work
statement28 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

# aws:PrincipalOrgID
statement29 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)

# aws:PrincipalOrgID Wildcard
statement30 = dict(
    Effect="Allow",
    Principal="*",
    Action=["rds:*"],
    Resource="*",
)


def test_statement_effect():
    statement = Statement(statement01)
    assert statement.effect == "Allow"


def test_statement_not_principal():
    statement = Statement(statement01)
    assert statement.uses_not_principal()


def test_statement_principals():
    statement = Statement(statement02)
    assert statement.principals == set(["arn:aws:iam::012345678910:root"])

    statement = Statement(statement03)
    assert statement.principals == set(["arn:aws:iam::012345678910:root"])

    statement = Statement(statement04)
    assert statement.principals == set(["arn:aws:iam::012345678910:root"])

    statement = Statement(statement06)
    assert statement.principals == set(["lambda.amazonaws.com"])

    statement_wo_principal = dict(statement06)
    del statement_wo_principal["Principal"]
    statement = Statement(statement_wo_principal)
    assert statement.principals == set([])
