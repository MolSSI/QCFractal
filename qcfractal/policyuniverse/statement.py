#     Copyright 2017 Netflix, Inc.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
"""
.. module: policyuniverse.statement
    :platform: Unix

.. version:: $$VERSION$$
.. moduleauthor::  Patrick Kelley <patrickbarrettkelley@gmail.com> @patrickbkelley

"""
# import re
from collections import namedtuple
# from netaddr import IPNetwork, IPAddress
# from time import strptime

PrincipalTuple = namedtuple("Principal", "category value")
# ConditionTuple = namedtuple("Condition", "category value")


class Statement(object):
    def __init__(self, statement):
        self.statement = statement
        # self.condition_entries = self._condition_entries()
        self.principals = self._principals()
        self.actions = self._actions()

    @property
    def effect(self):
        return self.statement.get("Effect")

    def _actions(self):
        actions = self.statement.get("Action")
        if not actions:
            return set()
        if not isinstance(actions, list):
            actions = [actions]
        return set(actions)

    def uses_not_principal(self):
        return "NotPrincipal" in self.statement

    @property
    def resources(self):
        if "NotResource" in self.statement:
            return set(["*"])

        resources = self.statement.get("Resource")
        if not isinstance(resources, list):
            resources = [resources]
        return set(resources)

    def whos_allowed(self):
        """Returns set containing any entries from principal and condition section.

        Example:

        statement = Statement(dict(
            Effect='Allow',
            Principal='arn:aws:iam::*:role/Hello',
            Action=['ec2:*'],
            Resource='*',
            Condition={
                'StringLike': {
                    'AWS:SourceOwner': '012345678910'
                }}))

        statement.whos_allowed()
        > set([
        >    PrincipalTuple(category='principal', value='arn:aws:iam::*:role/Hello'),
        >    ConditionTuple(category='account', value='012345678910')])
        """
        who = set()
        for principal in self.principals:
            principal = PrincipalTuple(category="principal", value=principal)
            who.add(principal)
        who = who.union(self.condition_entries)
        return who

    def _principals(self):
        """Extracts all principals from IAM statement.

        Should handle these cases:
        "Principal": "value"
        "Principal": ["value"]
        "Principal": { "AWS": "value" }
        "Principal": { "AWS": ["value", "value"] }
        "Principal": { "Service": "value" }
        "Principal": { "Service": ["value", "value"] }

        Return: Set of principals
        """
        principals = set()
        principal = self.statement.get("Principal", None)
        if not principal:
            # It is possible not to define a principal, AWS ignores these statements.
            return principals

        if isinstance(principal, dict):

            if "AWS" in principal:
                self._add_or_extend(principal["AWS"], principals)

            if "Service" in principal:
                self._add_or_extend(principal["Service"], principals)

        else:
            self._add_or_extend(principal, principals)

        return principals

    def _add_or_extend(self, value, structure):
        if isinstance(value, list):
            structure.update(set(value))
        else:
            structure.add(value)

    # def _condition_entries(self):
    #     """Extracts any ARNs, Account Numbers, UserIDs, Usernames, CIDRs, VPCs, and VPC Endpoints from a condition block.
    #
    #     Ignores any negated condition operators like StringNotLike.
    #     Ignores weak condition keys like referer, date, etc.
    #
    #     Reason: A condition is meant to limit the principal in a statement.  Often, resource policies use a wildcard principal
    #     and rely exclusively on the Condition block to limit access.
    #
    #     We would want to alert if the Condition had no limitations (like a non-existent Condition block), or very weak limitations.  Any negation
    #     would be weak, and largely equivelant to having no condition block whatsoever.
    #
    #     The alerting code that relies on this data must ensure the condition has at least one of the following:
    #     - A limiting ARN
    #     - Account Identifier
    #     - AWS Organization Principal Org ID
    #     - User ID
    #     - Source IP / CIDR
    #     - VPC
    #     - VPC Endpoint
    #
    #     https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_condition-keys.html
    #     """
    #     conditions = list()
    #     condition = self.statement.get("Condition")
    #     if not condition:
    #         return conditions
    #
    #     key_mapping = {
    #         "username": "username",
    #         "groupname": "groupname",
    #         "sourceip": "cidr",
    #         "accesstime": "access"
    #     }
    #
    #     relevant_condition_operators = [
    #         re.compile(
    #             "((ForAllValues|ForAnyValue):)?ARN(Equals|Like)(IfExists)?",
    #             re.IGNORECASE,
    #         ),
    #         re.compile(
    #             "((ForAllValues|ForAnyValue):)?String(Equals|Like)(IgnoreCase)?(IfExists)?",
    #             re.IGNORECASE,
    #         ),
    #         re.compile(
    #             "((ForAllValues|ForAnyValue):)?DateLessThan(IgnoreCase)?(IfExists)?",
    #             re.IGNORECASE,
    #         ),
    #         re.compile(
    #             "((ForAllValues|ForAnyValue):)?IpAddress(IfExists)?", re.IGNORECASE
    #         )
    #     ]
    #
    #     for condition_operator in condition.keys():
    #         if any(
    #             regex.match(condition_operator)
    #             for regex in relevant_condition_operators
    #         ):
    #             for key, value in condition[condition_operator].items():
    #                 # ForAllValues and ForAnyValue must be paired with a list.
    #                 # Otherwise, skip over entries.
    #                 if not isinstance(
    #                     value, list
    #                 ) and condition_operator.lower().startswith("for"):
    #                     continue
    #
    #                 if key.lower() in key_mapping:
    #                     if isinstance(value, list):
    #                         for v in value:
    #                             conditions.append(
    #                                 ConditionTuple(
    #                                     value=v, category=key_mapping[key.lower(
    #                                     )]
    #                                 )
    #                             )
    #                     else:
    #                         conditions.append(
    #                             ConditionTuple(
    #                                 value=value, category=key_mapping[key.lower(
    #                                 )]
    #                             )
    #                         )
    #     return conditions

    # @property
    # def condition_arns(self):
    #     return self._condition_field("arn")

    # @property
    # def condition_accounts(self):
    #     return self._condition_field("account")
    #
    # @property
    # def condition_orgids(self):
    #     return self._condition_field("org-id")
    #
    # @property
    # def condition_userids(self):
    #     return self._condition_field("userid")
    #
    # @property
    # def condition_cidrs(self):
    #     return self._condition_field("cidr")
    #
    # @property
    # def condition_accesses(self):
    #     return self._condition_field("access")
    #
    # @property
    # def condition_vpcs(self):
    #     return self._condition_field("vpc")
    #
    # @property
    # def condition_vpces(self):
    #     return self._condition_field("vpce")
    #
    # def _condition_field(self, field):
    #     return set(
    #         [entry.value for entry in self.condition_entries if entry.category == field]
    #     )

    def evaluate(self, context):
        if (self._evaluate_resource(context) and
            self._evaluate_action(context)):
            # self._evaluate_principal(context) and
            # self._evaluate_condition(context)):
            return self.effect == "Allow"
        else:
            return None

    def _evaluate_resource(self, context):
        return '*' in self.resources or context['Resource'] in self.resources

    def _evaluate_action(self, context):
        return '*' in self.actions or context['Action'] in self.actions

    def _evaluate_principal(self, context):
        return '*' in self.principals or context['Principal'] in self.principals

    # def _evaluate_condition(self, context):
    #     return self._evaluate_condition_cidr(context) and self._evaluate_condition_accesses(context)
    #
    # def _evaluate_condition_cidr(self, context):
    #     if not self.condition_cidrs:
    #         return True
    #
    #     allow = False
    #     for condition_cidr in self.condition_cidrs:
    #         if IPAddress(context['IpAddress']) in IPNetwork(condition_cidr):
    #             allow = True
    #             continue
    #     return allow
    #
    # def _evaluate_condition_accesses(self, context):
    #     if not self.condition_accesses:
    #         return True
    #
    #     allow = False
    #     for condition_access in self.condition_accesses:
    #         access_time = strptime(
    #             context['AccessTime'], '%Y-%m-%dT%H:%M:%S')
    #         condition_access_time = strptime(
    #             condition_access, '%Y-%m-%dT%H:%M:%S')
    #         if access_time < condition_access_time:
    #             allow = True
    #             continue
    #     return allow
