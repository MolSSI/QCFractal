"""
Assists in grabbing the requisite schema
"""

__all__ = ["get_table_indices", "format_result_indices"]

# Load molecule schema

# Collection and hash indices
_table_indices = {

    "collection": ("collection", "name"),
    "procedure": ("procedure", "program"),

    "molecule": ("molecule_hash", "molecular_formula"),
    "result": ("molecule", "program", "driver", "method", "basis", "options"),  # ** Renamed molecule_id
    "options": ("program", "name"),

    # "task_queue": ("status", "tag", "hash_index"),
    "task_queue": ("status", "tag", "base_result"),  # updated
    "service_queue": ("status", "tag", "hash_index"),
}  # yapf: disable



def get_table_indices(name):
    if name not in _table_indices:
        raise KeyError("Indices for {} not found.".format(name))
    return _table_indices[name]


def format_result_indices(data, program=None):
    if program is None:
        program = data["program"]
    return program, data["molecule"], data["driver"], data["method"], data["basis"], data["options"]
