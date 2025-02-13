"""
Credit: https://github.com/Netflix-Skunkworks/policyuniverse
Patrick Kelley <patrick@netflix.com>

"""

from collections import namedtuple


PrincipalTuple = namedtuple("PrincipalTuple", "category value")


class Statement(object):
    def __init__(self, statement):
        self.statement = statement
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
        """
        Returns set containing any entries from principal and condition section.
        """
        who = set()
        for principal in self.principals:
            principal = PrincipalTuple(category="principal", value=principal)
            who.add(principal)

        return who

    def _principals(self):
        """Extracts all principals from IAM statement.

        Should handle these cases:
        "Principal": "value"
        "Principal": ["value"]

        Return: Set of principals
        """
        principals = set()
        principal = self.statement.get("Principal", None)
        if not principal:
            # It is possible not to define a principal, AWS ignores these statements.
            return principals

        self._add_or_extend(principal, principals)

        return principals

    def _add_or_extend(self, value, structure):
        if isinstance(value, list):
            structure.update(set(value))
        else:
            structure.add(value)

    def evaluate(self, context):
        if self._evaluate_resource(context) and self._evaluate_action(context) and self._evaluate_principal(context):
            return self.effect == "Allow"
        else:
            return None

    def _evaluate_resource(self, context):
        return "*" in self.resources or context["Resource"] in self.resources

    def _evaluate_action(self, context):
        return "*" in self.actions or context["Action"] in self.actions

    def _evaluate_principal(self, context):
        if len(self.principals) == 0:
            return True
        else:
            return "*" in self.principals or context["Principal"] in self.principals
