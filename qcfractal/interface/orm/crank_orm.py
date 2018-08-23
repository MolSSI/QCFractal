"""
A ORM for Crank
"""


class CrankORM:
    def __init__(self, data):
        self.data = data

        self.id = str(self.data["id"])
        # print(json.dumps(self.data["crank_state"], indent=2))

    @classmethod
    def from_json(cls, data):
        self._meta = data["crank_meta"]

    def __repr__(self):
        return str(list(self.data))

