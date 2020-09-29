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
.. module: policyuniverse.policy
    :platform: Unix

.. version:: $$VERSION$$
.. moduleauthor::  Patrick Kelley <patrickbarrettkelley@gmail.com> @patrickbkelley

"""
from .statement import Statement
from collections import defaultdict


class Policy(object):
    def __init__(self, policy):
        self.policy = policy
        self.statements = []

        statement_structure = self.policy.get("Statement", [])
        if not isinstance(statement_structure, list):
            statement_structure = [statement_structure]

        for statement in statement_structure:
            self.statements.append(Statement(statement))

    @property
    def principals(self):
        principals = set()
        for statement in self.statements:
            principals = principals.union(statement.principals)
        return principals

    @property
    def condition_entries(self):
        condition_entries = set()
        for statement in self.statements:
            condition_entries = condition_entries.union(
                statement.condition_entries)
        return condition_entries

    def whos_allowed(self):
        allowed = set()
        for statement in self.statements:
            if statement.effect == "Allow":
                allowed = allowed.union(statement.whos_allowed())
        return allowed

    def evaluate(self, context):
        try:
            allow = False
            for statement in self.statements:
                if statement.evaluate(context) == False:
                    return False
                elif statement.evaluate(context) == True:
                    allow = True
                    continue
                else:
                    continue

            return allow
        except Exception:
            return False
