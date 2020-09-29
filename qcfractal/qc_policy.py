from policyuniverse.statement import ConditionTuple, PrincipalTuple
from policyuniverse.policy import Policy
policy05 = dict(
    Statement=[
        dict(
            Effect='Allow',
            Principal='hshahin@vt.edu',
            Action=['GET'],
            Resource='information',
            Condition={
                'IpAddress': {
                    'SourceIP': ['192.168.0.0/24', '193.168.0.0/24']
                },
                'DateLessThan': {
                    'AccessTime': '2020-01-01T00:00:01'
                }})
    ])

# "Condition": {"DateGreaterThan": {"aws:TokenIssueTime": "2020-01-01T00:00:01Z"}}


policy = Policy(policy05)
# assert policy.whos_allowed() == set([
#     PrincipalTuple(category='principal', value='arn:aws:iam::*:role/Hello'),
#     PrincipalTuple(category='principal', value='arn:aws:iam::012345678910:root'),
#     ConditionTuple(category='cidr', value='0.0.0.0/0'),
#     ConditionTuple(category='account', value='012345678910')
# ])

# assert policy.is_internet_accessible() == False
context = {
    "Principal": "hshahin@vt.edu",
    "Action": "GET",
    "Resource": "information",
    "IpAddress": "193.168.0.1",
    "AccessTime": "2019-01-01T00:00:01",
}

print(policy.evaluate(context))
